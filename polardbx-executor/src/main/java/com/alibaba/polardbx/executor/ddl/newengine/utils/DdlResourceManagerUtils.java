package com.alibaba.polardbx.executor.ddl.newengine.utils;

import com.alibaba.polardbx.executor.ddl.newengine.resource.DdlEngineResources;
import com.alibaba.polardbx.gms.node.GmsNodeManager;
import com.alibaba.polardbx.optimizer.config.table.ScaleOutPlanUtil;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class DdlResourceManagerUtils {
    public static final String DN_STORAGE = ":DN_STORAGE";
    public static final String DN_NETWORK = ":DN_NETWORK";
    public static final String DN_IO = ":DN_IO";
    public static final String DN_SYSTEM_LOCK = ":DN_SYSTEM_LOCK";
    public static final String DN_CPU = ":DN_CPU";
    public static final String CN_NETWORK = ":CN_NETWORK";
    public static final String CN_CPU = ":CN_CPU";
    public static final String MOVE_PARTITION_BEFORE_CHECK = ":MOVE_PARTITION_BEFORE_CHECK";

    public static final Set<String> validateResourcesNames = Arrays.stream(new String[] {
        DN_STORAGE, DN_NETWORK, DN_IO, DN_SYSTEM_LOCK,
        DN_CPU, CN_NETWORK, CN_CPU}).collect(Collectors.toSet());

    public static void initializeResources(DdlEngineResources resourceToAllocate) {
        List<String> readyStorageInsts = ScaleOutPlanUtil.getStorageInstReady();
        for (String storageInst : readyStorageInsts) {
            if (!resourceToAllocate.containsKey(storageInst)) {
                resourceToAllocate.preAllocate(storageInst, 100L);
                resourceToAllocate.preAllocate(storageInst + DN_NETWORK, 100L);
                resourceToAllocate.preAllocate(storageInst + DN_IO, 100L);
                resourceToAllocate.preAllocate(storageInst + DN_CPU, 100L);
                resourceToAllocate.preAllocate(storageInst + DN_SYSTEM_LOCK, 100L);

            }
        }
        if (!resourceToAllocate.containsKey(CN_CPU)) {
            resourceToAllocate.preAllocate(CN_CPU, 100L);
        }
        if (!resourceToAllocate.containsKey(CN_NETWORK)) {
            resourceToAllocate.preAllocate(CN_NETWORK, 100L);
        }
        List<GmsNodeManager.GmsNode> remoteNodeList = GmsNodeManager.getInstance().getRemoteNodes();
        List<String> remoteServers = remoteNodeList.stream().map(GmsNodeManager.GmsNode::getServerKey).collect(
            Collectors.toList());
        initializeResourcesForRemoteServer(resourceToAllocate, remoteServers);
    }

    public static void initializeResourcesForRemoteServer(DdlEngineResources resourceToAllocate,
                                                          List<String> remoteServers) {
        if (!resourceToAllocate.getWithRemoteServer()) {
            for (String remoteServer : remoteServers) {
                if (remoteServer == null) {
                    continue;
                }
                if (!resourceToAllocate.containsKey(remoteServer + CN_CPU)) {
                    resourceToAllocate.preAllocate(remoteServer + CN_CPU, 100L);
                }
                if (!resourceToAllocate.containsKey(remoteServer + CN_NETWORK)) {
                    resourceToAllocate.preAllocate(remoteServer + CN_NETWORK, 100L);
                }
            }
            resourceToAllocate.setWithRemoteServer(true);
        }
    }
}
