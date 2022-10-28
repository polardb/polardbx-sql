/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.qatest.ddl.auto.locality.LocalityTestCaseUtils;

import com.alibaba.polardbx.qatest.ddl.auto.locality.LocalityTestBase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import net.jcip.annotations.NotThreadSafe;
import org.apache.commons.lang.BooleanUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@NotThreadSafe
public class LocalityTestUtils {
    public static class StorageNodeBean {
        public String instance;
        public String status;
        public String instKind;
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

    public static class TopologyBean{
        public String partitionName;
        public String group;
        public String storageId;

        public TopologyBean(String partitionName, String group, String storageId){
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

    public static List<LocalityBean> getLocalityInfo(Connection tddlConnection) {
        final String sql = "show locality";

        List<LocalityBean> res = new ArrayList<>();
        try (ResultSet result = JdbcUtil.executeQuerySuccess(tddlConnection, sql)) {
            while (result.next()) {
                String objectType = result.getString("OBJECT_TYPE");
                String objectName = result.getString("OBJECT_NAME");
                String locality = result.getString("LOCALITY");
                String groupElement = result.getString("OBJECT_GROUP_ELEMENT");
                LocalityBean
                    bean = new LocalityBean(objectType, objectName, locality, groupElement);
                res.add(bean);
            }
//            LOG.info("getLocalityInfo" + res);
            return res;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }


    public static Map<String, String> generateNodeMap(List<String> storageNodeBeanList, List<String> nodeList){
        Map<String, String> nodeMap = new HashMap<>();
        nodeMap.put(nodeList.get(0), storageNodeBeanList.get(0));
        for(int i = 1; i < nodeList.size(); i++){
            nodeMap.put(nodeList.get(i), storageNodeBeanList.get(i));
        }
        return nodeMap;
    }

    public static List<String> getDatanodes(Connection tddlConnection) {
        List<LocalityTestUtils.StorageNodeBean> dnList = getStorageInfo(tddlConnection);
        return dnList.stream()
            .filter(x -> "MASTER".equals(x.instKind))
            .sorted(Comparator.comparing(o->o.deletable))
            .map(x -> x.instance)
            .collect(Collectors.toList());
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
