package com.alibaba.polardbx.optimizer.planmanager.hotevolution;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.parse.bean.SqlParameterized;

/**
 * all information needed to evolve the plan with hot gsi
 */
public class GsiEvolutionInfo {
    private final SqlParameterized sqlParameterized;
    private final ExecutionContext executionContext;
    private final ExecutionPlan executionPlan;
    private final String indexName;
    private String templateId;

    public GsiEvolutionInfo(SqlParameterized sqlParameterized, ExecutionContext executionContext,
                            ExecutionPlan executionPlan, String indexName, String templateId) {
        this.sqlParameterized = sqlParameterized;
        this.executionContext = executionContext.copyContextForOptimizer();
        this.executionPlan = executionPlan;
        this.indexName = indexName;
        this.templateId = templateId;
    }

    public SqlParameterized getSqlParameterized() {
        return sqlParameterized;
    }

    public ExecutionContext getExecutionContext() {
        return executionContext;
    }

    public ExecutionPlan getExecutionPlan() {
        return executionPlan;
    }

    public String getIndexName() {
        return indexName;
    }

    public String getTemplateId() {
        return templateId;
    }
}
