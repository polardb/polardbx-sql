package com.alibaba.polardbx.executor.ddl.newengine.resource;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlResourceManagerUtils;
import io.grpc.netty.shaded.io.netty.util.internal.StringUtil;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

public class DdlEngineResources {

    public static DdlEngineResources copyFrom(DdlEngineResources ddlEngineResources) {
        return new DdlEngineResources(ddlEngineResources.resources, null, false);
    }

    public static int calHashCode(Set<String> sets) {
        return StringUtil.join(",", sets).toString().hashCode();
    }

    public static String concatSubJobOwner(String schema, String tableGroupName, List<String> physicalPartitionNames) {
        Set<String> physicalPartitionNameSet = new TreeSet<>(physicalPartitionNames);
        int hashCode = calHashCode(physicalPartitionNameSet);
        return String.format("schema: %s, tableGroupName: %s, partitionNames: %d", schema, tableGroupName, hashCode);
    }

//    public static Map<Long, Boolean> coverredBefore = new ConcurrentHashMap<>();
//
//    public static Boolean markNotCoverredBefore(Long taskId) {
//        if (coverredBefore.containsKey(taskId)) {
//            return true;
//        } else {
//            coverredBefore.put(taskId, true);
//            return false;
//        }
//    }

    public static String extractHost(String resource) {
        int index = resource.lastIndexOf(":");
        if (index == -1) {
            return resource;
        } else if (index == 0) {
            return "MASTER_CN";
        } else {
            return resource.substring(0, index);
        }
    }

    public static String extractResourceType(String resource) {
        int index = resource.lastIndexOf(":");
        if (index == -1) {
            return "";
        } else {
            return resource.substring(index + 1, resource.length());
        }
    }

    public static String digestCoverInfo(DdlEngineResources resourceAcquired, DdlEngineResources ddlEngineResources) {
        Boolean covered = true;
        String digestInfoFmt = "required %s[amount=%d], residue %s[amount=%d, waitInfo=%s];";
        List<String> digestInfos = new ArrayList<>();
        for (String resourceName : resourceAcquired.resources.keySet()) {
            String fullResourceName = concateServerKeyAndResource(resourceAcquired.getServerKey(), resourceName);
            if (ddlEngineResources.resources.containsKey(fullResourceName)) {
                if (!ddlEngineResources.resources.get(fullResourceName)
                    .cover(resourceAcquired.resources.get(resourceName))) {
                    covered = false;
                    digestInfos.add(
                        String.format(digestInfoFmt, fullResourceName,
                            resourceAcquired.resources.get(resourceName).amount,
                            fullResourceName, ddlEngineResources.resources.get(resourceName).amount,
                            ddlEngineResources.resources.get(resourceName).getOwnerMap().toString()));
                }
            }
        }
        String digestInfo = StringUtil.join("\n", digestInfos).toString();
        return digestInfo;

    }

    public Map<String, ResourceContainer> getResources() {
        return resources;
    }

    public void setResources(
        Map<String, ResourceContainer> resources) {
        this.resources = resources;
    }

    public Map<String, ResourceContainer> resources = new ConcurrentHashMap<>();

    public String getServerKey() {
        return serverKey;
    }

    public void setServerKey(String serverKey) {
        this.serverKey = serverKey;
    }

    public String serverKey = null;

    public Boolean getWithRemoteServer() {
        return withRemoteServer;
    }

    public void setWithRemoteServer(Boolean withRemoteServer) {
        this.withRemoteServer = withRemoteServer;
    }

    public Boolean withRemoteServer = false;

    public DdlEngineResources() {
    }

    @JSONCreator
    public DdlEngineResources(Map<String, ResourceContainer> resources, String serverKey, Boolean withRemoteServer) {
        this.resources = resources;
        this.serverKey = serverKey;
        this.withRemoteServer = withRemoteServer;
    }

    public Boolean containsKey(String resourceName) {
        return resources.containsKey(resourceName);
    }

    public void request(String resourceName, Long amount) {
        resources.put(resourceName, new ResourceContainer(amount, null));
    }

    public void request(String resourceName, Long amount, String owner) {
        resources.put(resourceName, new ResourceContainer(amount, owner));
    }

    public void requestForce(String resourceName, Long amount, String owner) {
        resources.put(resourceName, new ResourceContainer(amount, owner, true));
    }

    public void requestPhaseLock(String resourceName, Long amount, String owner, int phaseLock) {
        resources.put(resourceName, new ResourceContainer(amount, owner, true, phaseLock));
    }

    public void preAllocate(String resourceName, Long amount) {
        resources.put(resourceName, new ResourceContainer(amount, null));
    }

    public void allocate(DdlEngineResources resourceAcquired) {
        for (String resourceName : resourceAcquired.resources.keySet()) {
            String fullResourceName = concateServerKeyAndResource(resourceAcquired.getServerKey(), resourceName);
            if (resources.containsKey(fullResourceName)) {
                resources.get(fullResourceName).allocate(resourceAcquired.resources.get(resourceName));
            }
        }
    }

    public void free(DdlEngineResources resourceAcquired) {
        for (String resourceName : resourceAcquired.resources.keySet()) {
            String fullResourceName = concateServerKeyAndResource(resourceAcquired.getServerKey(), resourceName);
            if (resources.containsKey(fullResourceName)) {
                resources.get(fullResourceName).free(resourceAcquired.resources.get(resourceName));
            }
        }
    }

    public Boolean cover(DdlEngineResources resourceAcquired, Boolean ignoreLack) {
        Boolean covered = true;
        for (String resourceName : resourceAcquired.resources.keySet()) {
            String fullResourceName = concateServerKeyAndResource(resourceAcquired.getServerKey(), resourceName);
            if (resources.containsKey(fullResourceName)) {
                if (!resources.get(fullResourceName).cover(resourceAcquired.resources.get(resourceName))) {
                    covered = false;
                    return covered;
                }
            } else {
                Boolean force = resourceAcquired.resources.get(resourceName).force;
                if (force != null && force) {
                    resources.put(resourceName, new ResourceContainer(100L, null));
                }
            }
        }
        return covered;
    }

    @Override
    public String toString() {
        return resources.entrySet().toString();
    }

    public static Boolean isServerResource(String resource) {
        return !StringUtils.isEmpty(resource) && resource.startsWith(":CN");
    }

    public static String concateServerKeyAndResource(String serverKey, String resource) {
        if (isServerResource(resource) && !StringUtils.isEmpty(serverKey)) {
            return serverKey + resource;
        } else {
            return resource;
        }
    }

    public static String normalizeServerKey(String serverKey) {
        return Optional.ofNullable(serverKey).orElse("");
    }

    public static Boolean isValidateResourceName(String resourceName) {
        for (String key : DdlResourceManagerUtils.validateResourcesNames) {
            if (!resourceName.endsWith(key)) {
                return false;
            }
        }
        return true;
    }

    public static String concateDnResourceName(Pair<String, Integer> sourceHostIpAndPort, String instId) {
        String fullResourceName =
            String.format("%s(%s:%s)", instId, sourceHostIpAndPort.getKey(), sourceHostIpAndPort.getValue());
        return fullResourceName;
    }
}
