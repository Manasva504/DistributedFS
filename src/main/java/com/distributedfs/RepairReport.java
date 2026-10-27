package com.distributedfs;

import java.util.List;
import java.util.stream.Collectors;

/**
 * What one repair run ({@link RepairService}) found and did, or, for a dry run, would do.
 *
 * @param dryRun                 true if nothing was changed and every count means "would"
 * @param replicationFactor      the copies each chunk should have
 * @param nodes                  each node's heartbeat state when the run started
 * @param filesChecked           committed files whose chunks were checked
 * @param filesSkipped           files left alone: a pending upload, a manifest without chunk
 *                               checksums (format 1), or a manifest that could not be read
 * @param chunksChecked          chunks of the checked files
 * @param chunksHealthy          chunks that already had exactly R good copies and no damaged one
 * @param copiesAdded            new good copies stored for chunks with too few
 * @param copiesReplaced         damaged copies deleted and replaced by a new good copy
 * @param copiesRemoved          extra good copies deleted (only with every node UP), and damaged
 *                               copies deleted that could not be replaced
 * @param chunksLost             chunks with no good copy, while every node was UP
 * @param chunksUncertain        chunks left as they were because a node failed or a store's outcome
 *                               was unknown, or because a node that may hold them is DOWN
 * @param chunksNotRepaired      chunks that still have too few copies because no further UP node could take one
 * @param trimSkipped            true if some chunk had extra copies that were not deleted because a node was not UP
 * @param nodesFailedDuringRepair nodes that failed during this run and were not used again in it
 * @param lost                   the IDs of the lost chunks (at most {@link #MAX_LISTED})
 * @param durationMillis         how long the run took
 */
record RepairReport(boolean dryRun, int replicationFactor, List<NodeState> nodes, int filesChecked, int filesSkipped,
                    int chunksChecked, int chunksHealthy, int copiesAdded, int copiesReplaced, int copiesRemoved,
                    int chunksLost, int chunksUncertain, int chunksNotRepaired, boolean trimSkipped,
                    List<Integer> nodesFailedDuringRepair, List<String> lost, long durationMillis) {

    /** At most this many lost chunk IDs are listed. */
    static final int MAX_LISTED = 100;

    /** A node's index and heartbeat state. */
    record NodeState(int index, HeartbeatMonitor.State state) {
    }

    /** The report as the JSON body of {@code POST /cluster/repair}. It holds no path and no exception text. */
    String toJson() {
        String nodeList = nodes.stream()
                .map(n -> "{\"index\":" + n.index() + ",\"state\":" + Json.quote(n.state().name()) + "}")
                .collect(Collectors.joining(","));
        String failed = nodesFailedDuringRepair.stream().map(String::valueOf).collect(Collectors.joining(","));
        String lostList = lost.stream().map(Json::quote).collect(Collectors.joining(","));
        return "{\"dryRun\":" + dryRun
                + ",\"replicationFactor\":" + replicationFactor
                + ",\"nodes\":[" + nodeList + "]"
                + ",\"filesChecked\":" + filesChecked
                + ",\"filesSkipped\":" + filesSkipped
                + ",\"chunksChecked\":" + chunksChecked
                + ",\"chunksHealthy\":" + chunksHealthy
                + ",\"copiesAdded\":" + copiesAdded
                + ",\"copiesReplaced\":" + copiesReplaced
                + ",\"copiesRemoved\":" + copiesRemoved
                + ",\"chunksLost\":" + chunksLost
                + ",\"chunksUncertain\":" + chunksUncertain
                + ",\"chunksNotRepaired\":" + chunksNotRepaired
                + ",\"trimSkipped\":" + trimSkipped
                + ",\"nodesFailedDuringRepair\":[" + failed + "]"
                + ",\"lost\":[" + lostList + "]"
                + ",\"durationMillis\":" + durationMillis + "}";
    }
}
