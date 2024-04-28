package com.alibaba.polardbx.gms.util;

import com.alibaba.polardbx.gms.node.GmsNodeManager;

/**
 * @author fangwu
 */
public class SyncUtil {

    /**
     * Choose the node which has the smallest id
     */
    public static boolean isNodeWithSmallestId() {
        GmsNodeManager gmsNodeManager = GmsNodeManager.getInstance();

        // Choose the node whose has the minimum id
        GmsNodeManager.GmsNode node = gmsNodeManager.getLocalNode();
        if (node == null) {
            return true;
        }
        long localNodeId = node.origId;
        for (GmsNodeManager.GmsNode remoteNode : gmsNodeManager.getRemoteNodes()) {
            if (localNodeId > remoteNode.origId) {
                return false;
            }
        }

        return true;
    }
}
