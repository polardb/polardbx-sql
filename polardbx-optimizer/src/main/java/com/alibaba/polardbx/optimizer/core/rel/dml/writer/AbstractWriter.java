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

package com.alibaba.polardbx.optimizer.core.rel.dml.writer;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.logger.support.LogFormat;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskPlanUtils;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.BaseQueryOperation;
import com.alibaba.polardbx.optimizer.core.rel.BaseTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.dml.Writer;
import org.apache.calcite.rel.core.TableModify.Operation;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * @author chenmo.cm
 */
public abstract class AbstractWriter implements Writer {

    private final Operation operation;
    protected static final Logger LOGGER = LoggerFactory.getLogger(ComplexTaskPlanUtils.REPLICATE_SQL_LOG);

    public AbstractWriter(Operation operation) {
        this.operation = operation;
    }

    @Override
    public Operation getOperation() {
        return operation;
    }

    @Override
    public List<Writer> getInputs() {
        return Collections.emptyList();
    }

    public boolean isInsert() {
        return operation == Operation.INSERT;
    }

    public boolean isReplace() {
        return operation == Operation.REPLACE;
    }

    public boolean isUpdate() {
        return operation == Operation.UPDATE;
    }

    public boolean isDelete() {
        return operation == Operation.DELETE;
    }

    public boolean isMerge() {
        return operation == Operation.MERGE;
    }

    protected void logReplicateSql(TableMeta tableMeta,
                                   String targetPhyGroup,
                                   BaseQueryOperation targetPhyPlan,
                                   ExecutionContext executionContext) {

        if (!executionContext.getParamManager().getBoolean(ConnectionParams.ENABLE_SCALE_OUT_GROUP_PHY_DML_LOG)) {
            return;
        }

        StringBuilder stringBuilder = new StringBuilder();
        for (Map.Entry<String, ComplexTaskMetaManager.ComplexTaskStatus> entry : tableMeta.getComplexTaskTableMetaBean()
            .getPartitionTableMetaMap().entrySet()) {
            stringBuilder.append(entry.getKey());
            stringBuilder.append("/");
            stringBuilder.append(entry.getValue().toString());
            stringBuilder.append(",");
        }

        String targetSql = targetPhyPlan.getNativeSql();

        String formatTargetSql = LogFormat.formatLog(targetSql);
        Map<Integer, ParameterContext> contextParams =
            executionContext.getParams() == null ? null : executionContext.getParams().getCurrentParameter();
        Pair<String, Map<Integer, ParameterContext>> dbIndexAndParam =
            targetPhyPlan.getDbIndexAndParam(contextParams, executionContext);

        Map<Integer, ParameterContext> params = dbIndexAndParam.getValue();

        JSONArray jsonArray = new JSONArray();
        if (params != null && params.size() > 0) {
            for (Map.Entry<Integer, ParameterContext> pair : params.entrySet()) {
                jsonArray.add(pair.getValue().getValue());
            }
        } else if (((BaseTableOperation) targetPhyPlan).getBatchParameters() != null) {
            for (Map<Integer, ParameterContext> item : ((BaseTableOperation) targetPhyPlan).getBatchParameters()) {
                for (Map.Entry<Integer, ParameterContext> pair : item.entrySet()) {
                    jsonArray.add(pair.getValue().getValue());
                }
            }
        }

        String paramsString = jsonArray.toJSONString();
        String scaleOutSqlLog = String
            .format("[traceId:%s] [tar:%s] [ltb/sts:%s/%s] %s [params:%s], [ExecutionContext:%s], [batchParameter:%s]",
                executionContext.getTraceId(),
                targetPhyGroup, tableMeta.getTableName(), stringBuilder.toString(),
                formatTargetSql,
                paramsString,
                executionContext.toString(),
                ((BaseTableOperation) targetPhyPlan).getBatchParameters() != null ?
                    String.valueOf(
                        System.identityHashCode(((BaseTableOperation) targetPhyPlan).getBatchParameters())) :
                    "");
        LOGGER.info(scaleOutSqlLog);

    }

    @Override
    public <C> C unwrap(Class<C> aClass) {
        if (aClass.isInstance(this)) {
            return (C) this;
        }
        return null;
    }
}
