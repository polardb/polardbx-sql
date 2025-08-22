package com.alibaba.polardbx.executor.ddl.job.task.gsi;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.polardbx.executor.ExecutorHelper;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.meta.TableMetaChanger;
import com.alibaba.polardbx.executor.ddl.job.task.basic.AlterTablePhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.spec.AlterTableRollbacker;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.gsi.GsiUtils;
import com.alibaba.polardbx.executor.spi.ITransactionManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import com.alibaba.polardbx.optimizer.parse.visitor.DrdsUnparameterizeSqlVisitor;
import com.alibaba.polardbx.optimizer.utils.SqlIdentifierUtil;
import lombok.Getter;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;

@Getter
@TaskName(name = "AlterGsiAddLocalIndexTask")
public class AlterGsiAddLocalIndexTask extends AlterTablePhyDdlTask {

    private String logicalTableName;
    private String indexTableName;

    @JSONCreator
    public AlterGsiAddLocalIndexTask(String schemaName,
                                     String logicalTableName,
                                     String indexTableName,
                                     PhysicalPlanData physicalPlanData) {
        super(schemaName, indexTableName, physicalPlanData);
        this.logicalTableName = logicalTableName;
        this.indexTableName = indexTableName;
        String sql = substitueQuestionToTableName(physicalPlanData.getSqlTemplate(), indexTableName);
        this.setSourceSql(sql);
        onExceptionTryRecoveryThenRollback();
    }

    @Override
    public void executeImpl(ExecutionContext executionContext) {
        ITransactionManager tm = ExecutorContext.getContext(schemaName).getTransactionManager();
        assignDdlConcurrentParams(executionContext);
        GsiUtils.wrapWithNoTrx(tm, executionContext, addLocalIndexEc -> {
            super.executeImpl(addLocalIndexEc);
            return null;
        });
    }

    void assignDdlConcurrentParams(ExecutionContext executionContext) {
        ParamManager paramManager = executionContext.getParamManager();
        if (paramManager.getBoolean(ConnectionParams.GSI_BACKFILL_OVERRIDE_DDL_PARAMS)) {
            executionContext.setOverrideDdlParams(true);
        }
    }

    @Override
    protected List<RelNode> genRollbackPhysicalPlans(ExecutionContext executionContext) {
        String sql = substitueQuestionToTableName(physicalPlanData.getSqlTemplate(), indexTableName);
        SQLAlterTableStatement alterTableStmt = (SQLAlterTableStatement) FastsqlUtils.parseSql(sql).get(0);
        String reversedSql = genReversedAlterTableStmt(alterTableStmt);
        return genReversedPhysicalPlans(reversedSql, executionContext);
    }

    public static String substitueQuestionToTableName(String sql, String logicalTableName) {
        // this is very evil, but there are no better way and it's absolutely right on this scene.
        // don't refer to it unless you know what you are doing like me.
        return sql.replace("ALTER TABLE ?", "ALTER TABLE " + SqlIdentifier.surroundWithBacktick(logicalTableName));
    }
}
