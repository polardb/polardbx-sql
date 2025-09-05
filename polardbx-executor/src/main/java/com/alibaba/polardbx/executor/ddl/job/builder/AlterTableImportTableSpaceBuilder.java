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

package com.alibaba.polardbx.executor.ddl.job.builder;

import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.SqlConverter;
import com.alibaba.polardbx.optimizer.core.rel.ReplaceTableNameWithQuestionMarkVisitor;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.DdlPreparedData;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.rel.ddl.AlterTableImportTableSpace;
import org.apache.calcite.sql.SqlAlterTableImportTableSpace;
import org.apache.calcite.sql.SqlDdlNodes;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class AlterTableImportTableSpaceBuilder extends DdlPhyPlanBuilder {

    final static String SQL_TEMPLATE = "ALTER TABLE ? IMPORT TABLESPACE";
    final static String SQL_TEMPLATE_IF_NOT_EXIST = "ALTER TABLE ? IMPORT TABLESPACE IF NOT EXISTS";

    public AlterTableImportTableSpaceBuilder(DDL ddl, DdlPreparedData preparedData,
                                             TreeMap<String, List<List<String>>> tableTopology,
                                             ExecutionContext executionContext) {
        super(ddl, preparedData, executionContext);
        this.tableTopology = tableTopology;
    }

    public static AlterTableImportTableSpaceBuilder createBuilder(String schemaName,
                                                                  String logicalTableName,
                                                                  boolean ifNotExists,
                                                                  TreeMap<String, List<List<String>>> tableTopology,
                                                                  ExecutionContext executionContext) {
        ReplaceTableNameWithQuestionMarkVisitor visitor =
            new ReplaceTableNameWithQuestionMarkVisitor(schemaName, executionContext);

        SqlIdentifier logicalTableNameNode = new SqlIdentifier(logicalTableName, SqlParserPos.ZERO);

        SqlAlterTableImportTableSpace
            sqlAlterTableImportTableSpace =
            ifNotExists ? SqlDdlNodes.alterTableImportTableSpace(logicalTableNameNode, SQL_TEMPLATE_IF_NOT_EXIST) :
                SqlDdlNodes.alterTableImportTableSpace(logicalTableNameNode, SQL_TEMPLATE);
        sqlAlterTableImportTableSpace = (SqlAlterTableImportTableSpace) sqlAlterTableImportTableSpace.accept(visitor);

        final RelOptCluster cluster = SqlConverter.getInstance(executionContext).createRelOptCluster(null);
        AlterTableImportTableSpace alterTableImportTableSpace =
            AlterTableImportTableSpace.create(sqlAlterTableImportTableSpace, logicalTableNameNode, cluster);

        DdlPreparedData preparedData = new DdlPreparedData();
        preparedData.setSchemaName(schemaName);
        preparedData.setTableName(logicalTableName);

        return new AlterTableImportTableSpaceBuilder(alterTableImportTableSpace, preparedData, tableTopology,
            executionContext);
    }

    @Override
    protected void buildTableRuleAndTopology() {
        boolean isNewPartDb = DbInfoManager.getInstance().isNewPartitionDb(ddlPreparedData.getSchemaName());
        if (!isNewPartDb) {
            buildExistingTableRule(ddlPreparedData.getTableName());
        }
    }

    @Override
    public void buildPhysicalPlans() {
        buildSqlTemplate();
        buildPhysicalPlans(ddlPreparedData.getTableName());
    }

}
