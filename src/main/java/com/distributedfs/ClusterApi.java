package com.distributedfs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * What the cluster endpoints of {@link FileApiHandler} answer: {@code GET /cluster/status} and
 * {@code POST /cluster/repair}. The status is built from the heartbeat's latest states only: it
 * reads no chunk, takes no storage lock and sends nothing to the nodes. Neither answer contains a
 * file path or exception text: local nodes are named by their folder's name, remote nodes by
 * their 127.0.0.1 address.
 */
final class ClusterApi {

    private final HeartbeatMonitor monitor;
    private final RepairService repair;
    private final int replicationFactor;
    private final List<String> addresses;

    ClusterApi(HeartbeatMonitor monitor, RepairService repair, int replicationFactor) {
        this.monitor = Objects.requireNonNull(monitor, "monitor must not be null");
        this.repair = Objects.requireNonNull(repair, "repair must not be null");
        this.replicationFactor = replicationFactor;
        List<String> list = new ArrayList<>();
        List<ChunkStore> nodes = monitor.nodes();
        for (int i = 0; i < nodes.size(); i++) {
            list.add(nodes.get(i) instanceof RemoteStorageNode remote ? "127.0.0.1:" + remote.port()
                    : "local folder node-" + i);
        }
        this.addresses = List.copyOf(list);
    }

    /** The body of {@code GET /cluster/status}. */
    String statusJson() {
        StringBuilder json = new StringBuilder("{\"replicationFactor\":").append(replicationFactor)
                .append(",\"heartbeatIntervalMillis\":").append(monitor.intervalMillis())
                .append(",\"downAfterFailures\":").append(monitor.failuresForDown())
                .append(",\"nodes\":[");
        List<HeartbeatMonitor.NodeHealth> states = monitor.snapshot();
        for (int i = 0; i < states.size(); i++) {
            HeartbeatMonitor.NodeHealth health = states.get(i);
            String nodeId = monitor.nodes().get(i).nodeId().orElse(null);
            json.append(i == 0 ? "" : ",")
                    .append("{\"index\":").append(i)
                    .append(",\"nodeId\":").append(nodeId == null ? "null" : Json.quote(nodeId))
                    .append(",\"address\":").append(Json.quote(addresses.get(i)))
                    .append(",\"state\":").append(Json.quote(health.state().name()))
                    .append(",\"lastGoodCheck\":")
                    .append(health.lastGoodCheck() == null ? "null" : Json.quote(health.lastGoodCheck().toString()))
                    .append(",\"failuresInARow\":").append(health.failuresInARow())
                    .append("}");
        }
        return json.append("]}").toString();
    }

    /**
     * Runs a repair, or a dry run, and returns the body of {@code POST /cluster/repair}.
     *
     * @throws RepairService.RepairInProgressException if a repair is already running
     */
    String repairJson(boolean dryRun) throws IOException {
        return repair.repair(dryRun).toJson();
    }
}
