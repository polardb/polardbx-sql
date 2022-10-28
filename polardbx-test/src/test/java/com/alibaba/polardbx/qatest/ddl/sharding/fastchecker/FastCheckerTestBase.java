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

package com.alibaba.polardbx.qatest.ddl.sharding.fastchecker;
import com.alibaba.polardbx.optimizer.config.table.Field;
import com.alibaba.polardbx.qatest.AsyncDDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Ignore;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by zhuqiwei.
 *
 * @author: zhuqiwei
 */

@Ignore
public class FastCheckerTestBase extends AsyncDDLBaseNewDBTestCase {
    protected static String schemaName = "fastCheckerTestDb";
    protected static String srcLogicTable = "srcTable";
    protected static int srcTbPartition = 2;
    protected static String srcTableTemplate = "create table {0} (\n"
            + " id int(11) NOT NULL,\n"
            + " c_user_id varchar(50),\n"
            + " c_name varchar(50),\n"
            + " c_province_id int(11),\n"
            + " create_time datetime,\n"
            + " primary key(id) )\n"
            + " DBPARTITION BY hash(id)  TBPARTITION BY hash(c_name)  TBPARTITIONS {1};";

    protected static String dstIsomorphicLogicTable = "dstTable1";
    protected static int dstIsomorphicLogicTbPartition = 2;
    protected static String dstIsoTableTemplate = "create table {0} (\n"
            + " id int(11) NOT NULL,\n"
            + " c_user_id varchar(50),\n"
            + " c_name varchar(50),\n"
            + " c_province_id int(11),\n"
            + " create_time datetime,\n"
            + " primary key(id) )\n"
            + " DBPARTITION BY hash(id)  TBPARTITION BY hash(c_name)  TBPARTITIONS {1};";

    protected static String dstHeterogeneousLogicTable = "dstTable2";
    protected static int dstHeterogeneousTbPartition = 8;
    protected static String dstHeteroTableTemplate = "create table {0} (\n"
            + " id int(11) NOT NULL,\n"
            + " c_user_id varchar(50),\n"
            + " primary key(id) )\n"
            + " DBPARTITION BY hash(id)  TBPARTITION BY hash(c_user_id)  TBPARTITIONS {1};";

    protected static List<String> fullColumns =
            ImmutableList.of("id", "c_user_id", "c_name", "c_province_id", "create_time");
    protected static List<String> partialColumns = ImmutableList.of("id", "c_user_id");

    protected static String fullColumnInsert = "insert into {0} values ({1}, {2}, {3}, {4}, {5})";
    protected static String fullCopyInsert = "insert into {0} select * from {1}";
    protected static String partialCopyInsert = "insert into {0} select {1}, {2} from {3}";

    protected static String IdleQuerySqlTemplate = "SELECT {0}, `_drds_implicit_id_`\n"
            + "FROM ?\n"
            + "LIMIT 1";

    protected static String HashcheckQuerySqlTemplate = "SELECT HASHCHECK({0}, `_drds_implicit_id_`)\n"
            + "FROM ?";

    public void prepareData() {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "drop database if exists " + quoteSpecialName(schemaName));
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create database " + quoteSpecialName(schemaName));
        JdbcUtil.executeUpdateSuccess(tddlConnection, "use " + schemaName);

        JdbcUtil.executeUpdateSuccess(tddlConnection,
                MessageFormat.format(srcTableTemplate, srcLogicTable, srcTbPartition));
        JdbcUtil.executeUpdateSuccess(tddlConnection,
                MessageFormat.format(dstIsoTableTemplate, dstIsomorphicLogicTable, dstIsomorphicLogicTbPartition));
        JdbcUtil.executeUpdateSuccess(tddlConnection,
                MessageFormat.format(dstHeteroTableTemplate, dstHeterogeneousLogicTable, dstHeterogeneousTbPartition));

        for (int i = 1; i <= 500; i++) {
            JdbcUtil.executeUpdateSuccess(tddlConnection, MessageFormat
                    .format(fullColumnInsert, srcLogicTable, i, "uuid()", "uuid()", "FLOOR(Rand() * 1000)", "now()"));
        }

        JdbcUtil.executeUpdateSuccess(tddlConnection,
                MessageFormat.format(fullCopyInsert, dstIsomorphicLogicTable, srcLogicTable));
        JdbcUtil.executeUpdateSuccess(tddlConnection,
                MessageFormat.format(partialCopyInsert, dstHeterogeneousLogicTable, "id", "c_user_id", srcLogicTable));

    }

    public void cleanData() {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "drop databases if exists " + quoteSpecialName(schemaName));
    }

    private RelDataType buildRelDatatype(List<String> columnsName, String tableName) {
        SqlTypeFactoryImpl typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        List<RelDataTypeField> fields = new ArrayList<>();

        for (int i = 0; i < columnsName.size(); i++) {
            String name = columnsName.get(i);
            Field field =
                    new Field(tableName, name, buildDataType(name, typeFactory), false, (name.equals("id") ? true : false));
            RelDataTypeFieldImpl relDataTypeField = new RelDataTypeFieldImpl(name, i, field.getRelType());
            fields.add(relDataTypeField);
        }

        RelDataType relDataType = new RelRecordType(StructKind.FULLY_QUALIFIED, fields);
        return relDataType;
    }

    private RelDataType buildDataType(String columnName, SqlTypeFactoryImpl typeFactory) {
        if (columnName.equals("id") || columnName.equals("c_province_id")) {
            return typeFactory.createSqlType(SqlTypeName.INTEGER);
        } else if (columnName.equals("c_user_id") || columnName.equals("c_name")) {
            return typeFactory.createSqlType(SqlTypeName.VARCHAR);
        } else {
            return typeFactory.createSqlType(SqlTypeName.DATETIME);
        }
    }

    protected Map<String, Set<String>> getStorageIds(String schemaName) {
        Map<String, Set<String>> storages = new HashMap<>();
        String tddlSql = "use " + schemaName;
        JdbcUtil.executeUpdate(tddlConnection, tddlSql);
        tddlSql = "show datasources";
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = JdbcUtil.preparedStatement(tddlSql, tddlConnection);
            rs = stmt.executeQuery();
            while (rs.next()) {
                String sid = rs.getString("STORAGE_INST_ID");
                String groupName = rs.getString("GROUP");
                if (sid.contains("meta")) {
                    continue;
                }
                if (storages.containsKey(groupName)) {
                    storages.get(groupName).add(sid);
                } else {
                    storages.put(groupName, new HashSet<>());
                    storages.get(groupName).add(sid);
                }
            }
        } catch (Throwable e) {
            e.printStackTrace();
        } finally {
            try {
                if (stmt != null) {
                    stmt.close();
                }
                if (rs != null) {
                    rs.close();
                }
            } catch (SQLException e) {
            }
        }
        return storages;
    }

    protected Map<String, Set<String>> getTableTopology(String logicalTable) {
        Map<String, Set<String>> groupAndTbMap = new HashMap<>();
        // Get topology from logicalTableName
        String showTopology = String.format("show topology from %s", logicalTable);
        PreparedStatement stmt = JdbcUtil.preparedStatement(showTopology, tddlConnection);
        ResultSet rs = null;
        try {
            rs = stmt.executeQuery();
            while (rs.next()) {
                String groupName = (String) rs.getObject(2);
                String phyTbName = (String) rs.getObject(3);
                if (groupAndTbMap.containsKey(groupName)) {
                    groupAndTbMap.get(groupName).add(phyTbName);
                } else {
                    HashSet<String> st = new HashSet<>();
                    st.add(phyTbName);
                    groupAndTbMap.put(groupName, st);
                }
            }
            rs.close();
            stmt.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return groupAndTbMap;
    }

}
