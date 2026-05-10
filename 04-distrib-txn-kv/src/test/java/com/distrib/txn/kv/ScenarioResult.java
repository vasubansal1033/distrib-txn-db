package com.distrib.txn.kv;

import clock.HybridTimestamp;
import com.tickloom.ProcessId;
import kv.MVCCStore;
import kv.OrderPreservingCodec;

import java.util.List;
import java.util.Map;

public class ScenarioResult {
    private final Map<String, HybridTimestamp> snapshots;
    private final Map<String, HybridTimestamp> commitTimestamps;
    private final Map<ProcessId, HybridTimestamp> nodeHlcSnapshots;
    private final Map<ProcessId, TransactionalStorageReplica> replicas;
    private final List<String> clientNames;
    private final TransactionalStorageReplicaTestSupport.TopologyScenario topology;

    ScenarioResult(
            Map<String, HybridTimestamp> snapshots,
            Map<String, HybridTimestamp> commitTimestamps,
            Map<ProcessId, HybridTimestamp> nodeHlcSnapshots,
            Map<ProcessId, TransactionalStorageReplica> replicas,
            List<String> clientNames,
            TransactionalStorageReplicaTestSupport.TopologyScenario topology
    ) {
        this.snapshots = snapshots;
        this.commitTimestamps = commitTimestamps;
        this.nodeHlcSnapshots = nodeHlcSnapshots;
        this.replicas = replicas;
        this.clientNames = clientNames;
        this.topology = topology;
    }

    public HybridTimestamp snapshotOf(String clientName) {
        HybridTimestamp ts = snapshots.get(clientName);
        if (ts == null) {
            throw new IllegalArgumentException("No snapshot recorded for client: " + clientName);
        }
        return ts;
    }

    public HybridTimestamp commitTsOf(String clientName) {
        HybridTimestamp ts = commitTimestamps.get(clientName);
        if (ts == null) {
            throw new IllegalArgumentException("No commit timestamp recorded for client: " + clientName);
        }
        return ts;
    }

    public HybridTimestamp hlcAt(TopologyConstraint.NodeRef node) {
        ProcessId processId = resolveNode(node);
        HybridTimestamp hlc = nodeHlcSnapshots.get(processId);
        if (hlc == null) {
            throw new IllegalArgumentException("No HLC snapshot recorded for node: " + processId);
        }
        return hlc;
    }

    public String committedValue(String key) {
        return committedValueForKey(resolvedKey());
    }

    public HybridTimestamp committedVersionTs(String key) {
        ProcessId keyOwner = topology.keyOwner();
        TransactionalStorageReplica replica = replicas.get(keyOwner);
        MVCCStore store = replica.committedStore();
        Map<HybridTimestamp, byte[]> versions = store.getVersionsUpTo(
                OrderPreservingCodec.encodeString(resolvedKey()),
                new HybridTimestamp(Long.MAX_VALUE, Integer.MAX_VALUE));
        return versions.keySet().stream()
                .max(HybridTimestamp::compareTo)
                .orElse(null);
    }

    private String committedValueForKey(String key) {
        ProcessId keyOwner = topology.keyOwner();
        TransactionalStorageReplica replica = replicas.get(keyOwner);
        MVCCStore store = replica.committedStore();
        return store.getAsOf(new kv.MVCCKey(
                OrderPreservingCodec.encodeString(key),
                new HybridTimestamp(Long.MAX_VALUE, Integer.MAX_VALUE)
        )).map(OrderPreservingCodec::decodeString).orElse(null);
    }

    private String resolvedKey() {
        return topology.key();
    }

    private ProcessId resolveNode(TopologyConstraint.NodeRef node) {
        if (node.isKeyOwner()) {
            return topology.keyOwner();
        }
        int index = clientNames.indexOf(node.clientName());
        if (index == 0) return topology.coordinator1();
        if (index == 1) return topology.coordinator2();
        throw new IllegalArgumentException("Unknown client in NodeRef: " + node.clientName());
    }
}
