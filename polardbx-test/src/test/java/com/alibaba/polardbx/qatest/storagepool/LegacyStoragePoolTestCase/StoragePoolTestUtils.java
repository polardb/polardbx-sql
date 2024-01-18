package com.alibaba.polardbx.qatest.storagepool.LegacyStoragePoolTestCase;

import com.alibaba.polardbx.qatest.ddl.auto.locality.LocalityTestBase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import lombok.Getter;
import net.jcip.annotations.NotThreadSafe;
import org.apache.commons.lang.BooleanUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@NotThreadSafe
public class StoragePoolTestUtils {
    public static class StorageNodeBean {

        @Getter
        public String instance;
        @Getter
        public String status;
        @Getter
        public String instKind;

        public boolean isDeletable() {
            return deletable;
        }

        @Getter
        public boolean deletable;
        public Map<String, LocalityTestBase.ReplicaBean> replicas;

        public StorageNodeBean(String instance, String instKind,
                               String status, boolean deletable, String replicaStr) {
            this.instance = instance;
            this.instKind = instKind;
            this.status = status;
            this.deletable = deletable;
            this.replicas = new HashMap<>();
            String[] ss = replicaStr.split(",");
            for (String s : ss) {
                LocalityTestBase.ReplicaBean replica = LocalityTestBase.ReplicaBean.from(s);
                this.replicas.put(replica.address, replica);
            }
        }

        @Override
        public String toString() {
            return "StorageNodeBean{" +
                "instance='" + instance + "'" +
                ", deletable=" + deletable +
                ", replicas=[" + replicas +
                "]}";
        }

    }

    public static class DsBean {
        public String dnId;
        public String phyDbName;
        public String dbName;

        public DsBean(String dnId, String phyDbName, String dbName) {
            this.dnId = dnId;
            this.phyDbName = phyDbName;
            this.dbName = dbName;
        }

        @Override
        public String toString() {
            return "DsBean{" +
                this.dbName + "," +
                this.phyDbName + "," +
                this.dnId +
                '}';
        }
    }

    public static class StoragePoolBean {
        public List<String> dnIds;
        public String storagePool;
        public String undeletableDn;

        public StoragePoolBean(List<String> dnIds, String storagePool, String undeletableDn) {
            this.dnIds = dnIds;
            this.storagePool = storagePool;
            this.undeletableDn = undeletableDn;
        }

        @Override
        public String toString() {
            return "StoragePoolBean{" +
                this.storagePool + "," +
                this.dnIds + "," +
                this.undeletableDn +
                '}';
        }
    }

    public static class TopologyBean {
        public String partitionName;
        public String group;
        public String storageId;

        public TopologyBean(String partitionName, String group, String storageId) {
            this.partitionName = partitionName;
            this.group = group;
            this.storageId = storageId;
        }

        @Override
        public String toString() {
            return "TopologyBean{" +
                this.partitionName + "," +
                this.group + "," +
                this.storageId +
                '}';
        }

    }

    public static class LocalityBean {
        public String objectType;
        public String objectName;
        public String groupElement;
        public String locality;

        public LocalityBean(String objectType, String objectName, String locality, String groupElement) {
            this.objectName = objectName;
            this.objectType = objectType;
            this.groupElement = groupElement;
            this.locality = locality;
        }

        @Override
        public String toString() {
            return "LocalityBean{" +
                "objectType='" + objectType + "'" +
                ", objectName='" + objectName + "'" +
                ", locality='" + locality + "'" +
                ", groupElements='" + groupElement + "'" +
                '}';
        }
    }

    public static Map<String, String> generateNodeMap(List<String> storageNodeBeanList, List<String> nodeList) {
        Map<String, String> nodeMap = new HashMap<>();
        nodeMap.put(nodeList.get(0), storageNodeBeanList.get(0));
        for (int i = 1; i < nodeList.size(); i++) {
            nodeMap.put(nodeList.get(i), storageNodeBeanList.get(i));
        }
        return nodeMap;
    }

    public static List<String> getDatanodes(Connection tddlConnection) {
        List<StorageNodeBean> dnList = getStorageInfo(tddlConnection)
            .stream()
            .filter(x -> "MASTER".equals(x.instKind)).collect(Collectors.toList());
//        Collections.sort(dnList, Comparator.comparing(StorageNodeBean::isDeletable)
//            .thenComparing(StorageNodeBean::getInstance));
        Collections.sort(dnList, Comparator.comparing(StorageNodeBean::getInstance));
        return dnList.stream().map(o -> o.instance).collect(Collectors.toList());
    }

    public static List<StorageNodeBean> getStorageInfo(Connection tddlConnection) {
        final String showStorageSql = "SHOW STORAGE REPLICAS";
        // STORAGE_INST_ID | polardbx-storage-1-master
        // LEADER_NODE     | 100.82.20.151:3318
        // IS_HEALTHY      | true
        // INST_KIND       | MASTER
        // DB_COUNT        | 3
        // GROUP_COUNT     | 5
        // STATUS          | 0
        // DELETABLE       | 1
        // REPLICAS        | LEADER/100.82.20.151:3318/az2,FOLLOWER(100.82.20.151:3308)(az1),FOLLOWER(100.82.20.151:3328)(az3)

        List<StorageNodeBean> res = new ArrayList<>();
        try (ResultSet result = JdbcUtil.executeQuerySuccess(tddlConnection, showStorageSql)) {
            while (result.next()) {
                String instance = result.getString(1);
                String instKind = result.getString(4);
                String status = result.getString(7);
                boolean deletable = BooleanUtils.toBoolean(result.getString(8));
                String replicaStr = result.getString(11);
                StorageNodeBean
                    storageNode = new StorageNodeBean(instance, instKind, status, deletable, replicaStr);
                res.add(storageNode);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
//        LOG.info("getStorageInfo: " + res);
        return res;
    }
}
