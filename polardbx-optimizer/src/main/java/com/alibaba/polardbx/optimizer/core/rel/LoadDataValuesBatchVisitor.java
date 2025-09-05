package com.alibaba.polardbx.optimizer.core.rel;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import com.google.common.base.Preconditions;
import groovy.lang.IntRange;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * @author minggong.zm 2018/1/26 Create a new SqlInsert and corresponding
 * Parameters, which contains only the rows for a specific physical table.
 */
public class LoadDataValuesBatchVisitor extends BuildInsertValuesVisitor {

    // built params for current insert
    private List<Map<Integer, ParameterContext>> outputBatchParams;
    private final List<Integer> paramIndexMapping = new ArrayList<>();

    public LoadDataValuesBatchVisitor(List<Integer> valueIndices, Parameters parameters,
                                      List<Map<Integer, ParameterContext>> outputBatchParams) {
        super(Optional.ofNullable(valueIndices)
                .orElse(new ArrayList<>(new IntRange(0, parameters.isBatch() ? parameters.getBatchSize() : 1))),
            parameters,
            null);
        this.outputBatchParams = outputBatchParams;
    }

    public SqlInsert visit(SqlInsert insert) {
        Preconditions.checkArgument(insert.getUpdateList() == null || insert.getUpdateList().getList().isEmpty(),
            "Do not support LOAD DATA with duplicated key update part");
        return super.visit(insert);
    }

    /**
     * Copy sharded rows by their indices.
     */
    protected SqlBasicCall buildSqlValues(SqlBasicCall values) {
        Preconditions.checkArgument(values.operandCount() == 1,
            "Do not support LOAD DATA with multi value parts");

        final SqlNode[] newValuesOperands = new SqlBasicCall[1];
        final SqlNode oldRow = values.getOperands()[0];

        // Build new row and initialize paramIndexMap
        newValuesOperands[0] = oldRow.accept(this);

        if (isBatch) {
            for (Integer valueIndex : valueIndices) {
                outputBatchParams.add(
                    getParamContextProjection(
                        parameters.getBatchParameters().get(valueIndex)));
            }
        } else {
            Preconditions.checkArgument(valueIndices.size() == 1);
            outputBatchParams.add(getParamContextProjection(parameters.getCurrentParameter()));
        }

        return new SqlBasicCall(values.getOperator(),
            newValuesOperands,
            values.getParserPosition(),
            values.isExpanded(),
            values.getFunctionQuantifier());
    }

    private @NotNull Map<Integer, ParameterContext> getParamContextProjection(
        Map<Integer, ParameterContext> currentParameter) {
        final Map<Integer, ParameterContext> paramContext = new HashMap<>();
        for (int newIndex = 0; newIndex < paramIndexMapping.size(); newIndex++) {
            final Integer oldIndex = paramIndexMapping.get(newIndex);
            final ParameterContext oldPc = currentParameter.get(oldIndex + 1);
            ParameterContext newPc = PlannerUtils.changeParameterContextIndex(oldPc, newIndex + 1);
            paramContext.put(newIndex + 1, newPc);
        }
        return paramContext;
    }

    public SqlDynamicParam visit(SqlDynamicParam dynamicParam) {
        int oldIndex = dynamicParam.getIndex();
        int newIndex = paramIndexMapping.size();

        paramIndexMapping.add(oldIndex);

        // DynamicParam.index + 2 = params.key
        return new SqlDynamicParam(newIndex - 1, SqlParserPos.ZERO, dynamicParam.getValue());
    }
}
