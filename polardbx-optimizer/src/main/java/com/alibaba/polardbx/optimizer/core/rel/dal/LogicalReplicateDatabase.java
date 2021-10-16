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

package com.alibaba.polardbx.optimizer.core.rel.dal;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.dal.Dal;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlMoveDatabase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LogicalReplicateDatabase extends LogicalDal {

    private String sourceDatabase;
    private String targetHostName;
    private int targetPort;
    private Map<String, LogicCreateTable> logicCreateTableMap = new HashMap<>();
    ;

    private LogicalReplicateDatabase(Dal dal) {
        super(dal, "", "", null);
        /*SqlMoveDatabase rdb = (SqlMoveDatabase) dal.getAst();
        sourceDatabase = rdb.getSourceDataBase();
        targetHostName = rdb.getTargetHostName();
        targetPort = rdb.getTargetPort();*/
    }

    public static LogicalReplicateDatabase create(Dal dal) {
        //assert dal.getAst() instanceof SqlMoveDatabase;
        return new LogicalReplicateDatabase(dal);
    }

    public String getSourceDatabase() {
        return sourceDatabase;
    }

    public String getTargetHostName() {
        return targetHostName;
    }

    public int getTargetPort() {
        return targetPort;
    }

    public SqlMoveDatabase getSqlReplicateDatabase() {
        return (SqlMoveDatabase) getNativeSqlNode();
    }

    public void setLogicCreateTableInfo(String logicTableName, List<String> phyTableNames,
                                        String primaryTableDefinition, SqlCreateTable primaryTableNode) {
        if (logicCreateTableMap.get(logicTableName) == null) {
            LogicCreateTable logicCreateTable =
                new LogicCreateTable(logicTableName, phyTableNames, primaryTableDefinition, primaryTableNode);
            logicCreateTableMap.put(logicTableName, logicCreateTable);
        }
    }

    public List<RelNode> getCreateTableNodes(String logicTableName) {
        LogicCreateTable logicCreateTable = logicCreateTableMap.get(logicTableName);
        if (logicCreateTable != null) {
            Map<String, List<List<String>>> targetTables = new HashMap<>();
            List<List<String>> phyTables = new ArrayList<>();
            phyTables.add(logicCreateTable.getPhyTableNames());
            targetTables.put("group_key", phyTables);
        }
        return null;
    }

    @Override
    protected String getExplainName() {
        return "LogicalReplicateDatabase";
    }

    class LogicCreateTable {
        private String logicTableName;
        private List<String> phyTableNames;
        private String primaryTableDefinition;
        private SqlCreateTable primaryTableNode;

        public LogicCreateTable(String logicTableName, List<String> phyTableNames,
                                String primaryTableDefinition, SqlCreateTable primaryTableNode) {
            this.logicTableName = logicTableName;
            this.phyTableNames = phyTableNames;
            this.primaryTableDefinition = primaryTableDefinition;
            this.primaryTableNode = primaryTableNode;
        }

        public void setLogicTableName(String logicTableName) {
            this.logicTableName = logicTableName;
        }

        public String getLogicTableName() {
            return logicTableName;
        }

        public void setPhyTableNames(List<String> phyTableNames) {
            this.phyTableNames = phyTableNames;
        }

        public List<String> getPhyTableNames() {
            return phyTableNames;
        }

        public void setPrimaryTableDefinition(String primaryTableDefinition) {
            this.primaryTableDefinition = primaryTableDefinition;
        }

        public String getPrimaryTableDefinition() {
            return primaryTableDefinition;
        }

        public void setPrimaryTableNode(SqlCreateTable primaryTableNode) {
            this.primaryTableNode = primaryTableNode;
        }

        public SqlCreateTable getPrimaryTableNode() {
            return primaryTableNode;
        }
    }

}
