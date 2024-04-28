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

public class AlterTableImportTableSpaceBuilder extends DdlPhyPlanBuilder {

    final static String SQL_TEMPLATE = "ALTER TABLE ? IMPORT TABLESPACE";
    final static String SQL_TEMPLATE_IF_NOT_EXIST = "ALTER TABLE ? IMPORT TABLESPACE IF NOT EXISTS";

    public AlterTableImportTableSpaceBuilder(DDL ddl, DdlPreparedData preparedData,
                                             Map<String, List<List<String>>> tableTopology,
                                             ExecutionContext executionContext) {
        super(ddl, preparedData, executionContext);
        this.tableTopology = tableTopology;
    }

    public static AlterTableImportTableSpaceBuilder createBuilder(String schemaName,
                                                                  String logicalTableName,
                                                                  boolean ifNotExists,
                                                                  Map<String, List<List<String>>> tableTopology,
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
