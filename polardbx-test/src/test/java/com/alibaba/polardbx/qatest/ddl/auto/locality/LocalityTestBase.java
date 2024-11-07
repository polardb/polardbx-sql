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

package com.alibaba.polardbx.qatest.ddl.auto.locality;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.ha.impl.StorageRole;
import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import lombok.Value;
import org.apache.commons.lang.BooleanUtils;
import org.junit.Assert;
import org.junit.Before;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

public abstract class LocalityTestBase extends BaseTestCase {

    public static final Logger LOG = LoggerFactory.getLogger(LocalityTestBase.class);

    protected Connection tddlConnection;

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    @Before
    public void useConnection() {
        tddlConnection = getPolardbxConnection();
    }

    public String generateTableName() {
        final int tableNameLength = 10;
        final char lowerBound = 'a';
        final char upperBound = 'z';
        final int range = upperBound - lowerBound;

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < tableNameLength; i++) {
            char ch = (char) (ThreadLocalRandom.current().nextInt(range) + lowerBound);
            sb.append(ch);
        }
        return sb.toString();
    }

    protected TableDetails queryTableDetails(String schema, String tableName) throws SQLException {
        return queryTableDetails(schema, tableName, tddlConnection);
    }

    public String quote(String str) {
        return (str != null ? "\"" + str + "\"" : null);
    }

    /**
     * Query the approximate data length of the table
     */
    protected TableDetails queryTableDetails(String schema, String tableName, Connection conn) throws SQLException {
        final String fromClause = " from information_schema.table_detail";
        final String whereClause =
            " where TABLE_SCHEMA = " + quote(schema) + " and table_name = " + quote(tableName);
        final String summarySql = "select table_group_name, sum(data_length), sum(index_length), sum(table_rows) "
            + fromClause + whereClause;
        final String selectColumns =
            " partition_name, table_rows, data_length, index_length, bound_value, storage_inst_id ";
        final String partitionSql = "select" + selectColumns + fromClause + whereClause;

        TableDetails result = new TableDetails();

        try (ResultSet rs = JdbcUtil.executeQuerySuccess(conn, summarySql)) {
            Assert.assertTrue(rs.next());
            result.tableName = tableName;
            result.tableGroup = rs.getString(1);
            result.dataLength = rs.getLong(2);
            result.indexLength = rs.getLong(3);
            result.tableRows = rs.getLong(4);
        }
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(conn, partitionSql)) {
            while (rs.next()) {
                String partitionName = rs.getString(1);
                long rows = rs.getLong(2);
                long dataLength = rs.getLong(3);
                long indexLength = rs.getLong(4);
                String boundValue = rs.getString(5);
                String storageInstId = rs.getString(6);
                PartitionDetail pd =
                    new PartitionDetail(partitionName, rows, dataLength, indexLength, boundValue, storageInstId);
                result.partitions.add(pd);
            }
        }

        return result;
    }

    protected String genSplitPartitionSql(String table, int maxSize, int maxStep) {
        final String splitPartition = "rebalance table %s policy='split_partition' max_size=%d max_actions=%d "
            + "async=false";
        return String.format(splitPartition, table, maxSize, maxStep);
    }

    protected String genMergePartitionSql(String table, int maxSize) {
        final String mergePartition = "rebalance table %s policy='merge_partition' max_size=%d async=false";
        return String.format(mergePartition, table, maxSize);
    }

    public List<StorageNodeBean> getStorageInfo() {
        final String showStorageSql = "SHOW STORAGE REPLICAS";

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
        LOG.info("getStorageInfo: " + res);
        return res;
    }

    public List<String> getDatanodes() {
        List<StorageNodeBean> dnList = getStorageInfo();
        return dnList.stream()
            .filter(x -> "MASTER".equals(x.instKind))
            .sorted(Comparator.comparing(o -> o.deletable))
            .map(x -> x.instance)
            .collect(Collectors.toList());
    }

    public List<String> getDatanodesForDelete() {
        List<StorageNodeBean> dnList = getStorageInfo();
        return dnList.stream()
            .filter(x -> "MASTER".equals(x.instKind) && x.deletable)
            .map(x -> x.instance)
            .collect(Collectors.toList());
    }

    public static class LocalityBean {
        public String objectType;
        public String objectName;
        public String groupElement;
        public String locality;

        public LocalityBean(String objectType, String objectName, String groupElement, String locality) {
            this.objectName = objectName;
            this.objectType = objectType;
            this.groupElement = groupElement;
            this.locality = locality;
        }

        @Override
        public String toString() {
            return "LocalityBean{" +
                "objectType='" + objectType + '\'' +
                ", objectName='" + objectName + '\'' +
                ", locality='" + locality + '\'' +
                ", groupElements='" + groupElement + '\'' +
                '}';
        }
    }

    /**
     * Choose a random datanode from cluster
     */

    public String chooseDatanode() {
        return chooseDatanode(false);
    }

    public String chooseDatanode(Boolean firstDn) {
        List<String> dnList = getDatanodes();
        if (dnList.isEmpty()) {
            throw new RuntimeException("datanode is empty");
        }
        Random random = new Random();
        return firstDn ? dnList.get(0) : dnList.get(random.nextInt(dnList.size()));
    }

    public List<LocalityBean> getLocalityBeanInfos() {
        return getLocalityInfo();
    }

    //    +-----------+--------------+-------------+--------------------------------------------------------+----------------------+
//    | OBJECT_ID | OBJECT_NAME  | OBJECT_TYPE | LOCALITY                                               | OBJECT_GROUP_ELEMENT |
//    +-----------+--------------+-------------+--------------------------------------------------------+----------------------+
//    | 127       | db1          | database    |                                                        |                      |
//    | 40739     | c0           | table       |                                                        |                      |
//    | 40741     | c1           | table       |                                                        |                      |
//    | 40735     | s1           | table       | dn=polardbx-storage-1-master                           |                      |
//    | 40736     | t1           | table       | dn=polardbx-storage-0-master,polardbx-storage-1-master |                      |
//    | 40737     | t2           | table       | dn=polardbx-storage-0-master,polardbx-storage-1-master |                      |
//    | 40738     | t3           | table       | dn=polardbx-storage-0-master                           |                      |
//    | 224       | single_tg    | tablegroup  |                                                        | c0                   |
//    | 226       | broadcast_tg | tablegroup  |                                                        | c1                   |
//    | 220       | single_tg220 | tablegroup  | dn=polardbx-storage-1-master                           | s1                   |
//    | 221       | tg221        | tablegroup  | dn=polardbx-storage-0-master,polardbx-storage-1-master | t1,t2                |
//    | 222       | tg222        | tablegroup  | dn=polardbx-storage-0-master                           | t3                   |
//    +-----------+--------------+-------------+--------------------------------------------------------+----------------------+
    public List<LocalityBean> getLocalityInfo() {
        final String sql = "show locality";

        List<LocalityBean> res = new ArrayList<>();
        try (ResultSet result = JdbcUtil.executeQuerySuccess(tddlConnection, sql)) {
            while (result.next()) {
                String objectType = result.getString("OBJECT_TYPE");
                String objectName = result.getString("OBJECT_NAME");
                String locality = result.getString("LOCALITY");
                String groupElement = result.getString("OBJECT_GROUP_ELEMENT");
                LocalityBean
                    bean = new LocalityBean(objectType, objectName, groupElement, locality);
                res.add(bean);
            }
            LOG.info("getLocalityInfo" + res);
            return res;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public List<DSBean> getDsListOfSchema(String schema) {
        return getDsList().stream()
            .filter(x -> x.database.equalsIgnoreCase(schema))
            .collect(Collectors.toList());
    }

    public List<DSBean> getDsBeanList() {
        return getDsList();
    }

    public List<String> getDnListOfDb(String dbName, boolean includeSingleGroup) {
        final String sql = "show ds where db = '" + dbName.replaceAll("`", "") + "'";
        Set<String> dnList = new HashSet<>();
        try (ResultSet result = JdbcUtil.executeQuerySuccess(tddlConnection, sql)) {
            while (result.next()) {
                String groupName = result.getString(4);
                if (includeSingleGroup || !groupName.endsWith("SINGLE_GROUP")) {
                    dnList.add(result.getString(2));
                }
            }
            return new ArrayList<>(dnList);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public List<DSBean> getDsList() {
        final String sql = "show ds";
        List<DSBean> res = new ArrayList<>();

        try (ResultSet result = JdbcUtil.executeQuerySuccess(tddlConnection, sql)) {
            while (result.next()) {
                String dn = result.getString(2);
                String db = result.getString(3);
                String group = result.getString(4);
                String physical = result.getString(5);
                DSBean
                    ds = new DSBean(dn, db, group, physical);
                res.add(ds);
            }
            return res;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * +----+-------------------+------------+
     * | ID | GROUP_NAME        | TABLE_NAME |
     * +----+-------------------+------------+
     * | 0  | HEHE_000000_GROUP | gg         |
     * +----+-------------------+------------+
     */
    public List<String> getDnListOfTable(String dbName, String tableName) {
        final String sql = "show topology from " + tableName;
        List<String> groups = new ArrayList<>();

        try (ResultSet result = JdbcUtil.executeQuerySuccess(tddlConnection, sql)) {
            while (result.next()) {
                String groupName = result.getString(2);
                groups.add(groupName);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        List<DSBean> dsList = getDsList();
        List<String> dnList = new ArrayList<>();
        for (String group : groups) {
            String dn = dsList.stream().filter(ds -> ds.group.equals(group)).findFirst().get().storageInst;
            dnList.add(dn);
        }
        return dnList;
    }

    /**
     * +----+---------------------------+--------------------+---------------------------------+----------------------+---------+
     * | ID | STORAGE_INST_ID           | DB                 | GROUP                           | PHY_DB               | MOVABLE |
     * +----+---------------------------+--------------------+---------------------------------+----------------------+---------+
     * | 0  | polardbx-storage-0-master | hehe               | HEHE_000000_GROUP               | hehe_000000          | 1       |
     * | 1  | polardbx-storage-0-master | hehe               | HEHE_000001_GROUP               | hehe_000001          | 1       |
     * | 2  | polardbx-storage-0-master | hehe               | HEHE_000002_GROUP               | hehe_000002          | 1       |
     * | 3  | polardbx-storage-0-master | hehe               | HEHE_000006_GROUP               | hehe_000006          | 1       |
     * | 4  | polardbx-storage-0-master | hehe               | HEHE_000007_GROUP               | hehe_000007          | 1       |
     * | 5  | polardbx-storage-0-master | hehe               | HEHE_000008_GROUP               | hehe_000008          | 1       |
     * | 6  | polardbx-storage-0-master | hehe               | HEHE_SINGLE_GROUP               | hehe_single          | 0       |
     * | 7  | polardbx-storage-1-master | hehe               | HEHE_000003_GROUP               | hehe_000003          | 1       |
     */
    @Value
    public static class DSBean {
        public String storageInst;
        public String database;
        public String group;
        public String physicalDb;

    }

    public static class PartitionDetail {
        public String partitionName;
        public long dataLength;
        public long indexLength;
        public long rows;
        public String bound;

        public String storageInstId;

        public PartitionDetail(String partitionName, long rows, long dataLength, long indexLength, String bound,
                               String storageInstId) {
            this.partitionName = partitionName;
            this.dataLength = dataLength;
            this.indexLength = indexLength;
            this.rows = rows;
            this.bound = bound;
            this.storageInstId = storageInstId;
        }

        @Override
        public String toString() {
            return "PartitionDetail{" +
                "partitionName='" + partitionName + '\'' +
                ", dataLength=" + dataLength +
                ", indexLength=" + indexLength +
                ", rows=" + rows +
                ", bound='" + bound +
                ", storageInstId='" + storageInstId + '\'' +
                '}';
        }
    }

    public static class TableDetails {
        public String tableGroup;
        public String tableName;
        public long dataLength;
        public long indexLength;
        public long tableRows;
        public List<PartitionDetail> partitions = new ArrayList<>();

        public TableDetails() {
        }

        @Override
        public String toString() {
            return "TableDetails{" +
                "tableGroup='" + tableGroup + "'" +
                ", tableName='" + tableName + "'" +
                ", dataLength=" + dataLength +
                ", indexLength=" + indexLength +
                ", tableRows=" + tableRows +
                ", partitions=" + partitions +
                '}';
        }
    }

    public static class ReplicaBean {
        public String address;
        public StorageRole role;
        public String zone;

        public static ReplicaBean from(String str) {
            String[] ss = str.split("/");
            if (ss.length < 3) {
                throw new RuntimeException("illegal replica format: " + str);
            }
            ReplicaBean result = new ReplicaBean();
            result.address = ss[1];
            result.role = StorageRole.getStorageRoleByString(ss[0]);
            result.zone = ss[2];
            return result;
        }

        @Override
        public String toString() {
            return "Replica{" + role + "/" + address + "/" + zone;
        }
    }

    /**
     * A simplified representation of StorageNode
     */
    public static class StorageNodeBean {
        public String instance;
        public String status;
        public String instKind;
        public boolean deletable;
        public Map<String, ReplicaBean> replicas;

        public StorageNodeBean(String instance, String instKind,
                               String status, boolean deletable, String replicaStr) {
            this.instance = instance;
            this.instKind = instKind;
            this.status = status;
            this.deletable = deletable;
            this.replicas = new HashMap<>();
            String[] ss = replicaStr.split(",");
            for (String s : ss) {
                ReplicaBean replica = ReplicaBean.from(s);
                this.replicas.put(replica.address, replica);
            }
        }

        @Override
        public String toString() {
            return "StorageNodeBean{" +
                "instance='" + instance + '\'' +
                ", deletable=" + deletable +
                ", replicas=[" + replicas +
                "]}";
        }

    }

}
