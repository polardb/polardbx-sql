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

package com.alibaba.polardbx.repo.mysql.handler;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.polardbx.common.TddlConstants;
import com.alibaba.polardbx.common.constants.SequenceAttribute;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.logger.support.LogFormat;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.BaseQueryOperation;
import com.alibaba.polardbx.optimizer.core.rel.BaseTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.DirectMultiDBTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.DirectTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.PhyDdlTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.ReplaceSequenceWithLiteralVisitor;
import com.alibaba.polardbx.optimizer.core.rel.SingleTableOperation;
import com.alibaba.polardbx.optimizer.utils.PhyTableOperationUtil;
import com.alibaba.polardbx.repo.mysql.spi.MyPhyTableModifyCursor;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.commons.lang.StringUtils;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by minggong.zm on 18/1/23.
 * <p>
 * PhyTableOperation or SingleTableOperation or DirectTableOperation.
 */
public class MySingleTableModifyHandler extends HandlerCommon {

    private static final Logger logger = LoggerFactory.getLogger(MySingleTableModifyHandler.class);
    private static final Logger scaleOutSqlLogger = SQLRecorderLogger.scaleOutSqlLogger;

    public MySingleTableModifyHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        Cursor cursor;
        try {
            cursor = handleInner(logicalPlan, executionContext);
        } catch (Throwable t) {
            if (logicalPlan instanceof BaseTableOperation) {
                ((BaseTableOperation) logicalPlan).setSuccessExecuted(false);
            }
            throw t;
        }
        return cursor;
    }

    protected Cursor handleInner(RelNode logicalPlan, ExecutionContext executionContext) {
        if (logicalPlan instanceof PhyTableOperation) {
            if (((PhyTableOperation) logicalPlan).isOnlyOnePartitionAfterPruning()) {
                PhyTableOperationUtil.enableIntraGroupParallelism(((BaseTableOperation) logicalPlan).getSchemaName(),
                    executionContext);
            }
        } else {
            if (logicalPlan instanceof DirectMultiDBTableOperation) {
                checkUpdateDeleteLimitLimitation(((DirectMultiDBTableOperation) logicalPlan).getNativeSqlNode(),
                    executionContext);
                PhyTableOperationUtil.enableIntraGroupParallelism(
                    ((DirectMultiDBTableOperation) logicalPlan).getBaseSchemaName(executionContext),
                    executionContext);
            } else if (logicalPlan instanceof BaseTableOperation && !(logicalPlan instanceof PhyDdlTableOperation)) {
                if (logicalPlan instanceof DirectTableOperation) {
                    checkUpdateDeleteLimitLimitation(
                        ((DirectTableOperation) logicalPlan).getNativeSqlNode(), executionContext);
                }
                PhyTableOperationUtil.enableIntraGroupParallelism(((BaseTableOperation) logicalPlan).getSchemaName(),
                    executionContext);
            }
        }
        MyPhyTableModifyCursor modifyCursor = (MyPhyTableModifyCursor) repo.getCursorFactory()
            .repoCursor(executionContext, logicalPlan);
        long oldLastInsertId = executionContext.getConnection().getLastInsertId();
        Long[] result = handleWithSequence(logicalPlan, executionContext);

        Long lastInsertId = null, returnedLastInsertId = null;
        if (result != null) {
            lastInsertId = result[0];
            returnedLastInsertId = result[1];
        }

        int[] affectRows;
        try {
            affectRows = modifyCursor.batchUpdate();
        } catch (Throwable e) {
            // Record UGSI table name if needed.
            if (e instanceof TddlRuntimeException && logicalPlan instanceof BaseTableOperation) {
                final TddlRuntimeException exception = (TddlRuntimeException) e;
                final BaseTableOperation operation = (BaseTableOperation) logicalPlan;
                if (1062 == exception.getErrorCode() && operation.getParent() instanceof TableModify &&
                    exception.getCause() instanceof SQLException) {
                    final TableModify modify = operation.getParent();
                    if (modify.getTable() != null && modify.getTable() instanceof RelOptTableImpl &&
                        ((RelOptTableImpl) modify.getTable()).getImplTable() instanceof TableMeta) {
                        final TableMeta meta = (TableMeta) ((RelOptTableImpl) modify.getTable()).getImplTable();
                        if (meta.isGsi() && meta.getGsiTableMetaBean() != null
                            && meta.getGsiTableMetaBean().gsiMetaBean != null &&
                            !meta.getGsiTableMetaBean().gsiMetaBean.nonUnique) {
                            // Duplicate entry on GSI.
                            // Replace physical unique index name to GSI table name.
                            final List<String> uniqueConstraint = new ArrayList<>();
                            if (meta.hasGsiImplicitPrimaryKey()) {
                                uniqueConstraint.add(TddlConstants.UGSI_PK_INDEX_NAME);
                            } else {
                                uniqueConstraint.add(meta.getPrimaryIndex().getPhysicalIndexName().toLowerCase());
                            }
                            if (meta.getSecondaryIndexes() != null) {
                                meta.getSecondaryIndexes()
                                    .forEach(item -> uniqueConstraint.add(item.getPhysicalIndexName().toLowerCase()));
                            }
                            for (String unique : uniqueConstraint) {
                                int idx = exception.getMessage().toLowerCase().indexOf(unique);
                                if (idx != -1) {
                                    // Found.
                                    final String newMsg;
                                    if (idx > 0 && '\'' == exception.getMessage().charAt(idx - 1)) {
                                        newMsg =
                                            exception.getMessage().substring(0, idx - 1) + "UGSI '" +
                                                meta.getTableName() +
                                                exception.getMessage().substring(idx + unique.length());
                                    } else {
                                        newMsg =
                                            exception.getMessage().substring(0, idx) + "UGSI " + meta.getTableName()
                                                + exception.getMessage().substring(idx + unique.length());
                                    }
                                    e = new TddlRuntimeException(exception.getErrorCodeType(), newMsg, exception);
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            // If exception happens, reset last insert id.
            executionContext.getConnection().setLastInsertId(oldLastInsertId);
            throw GeneralUtil.nestedException(e);
        }

        // loo all physical dml log if need
        logAllPhyDmlSqlIfNeed(executionContext, logicalPlan, affectRows);
        if (logicalPlan instanceof SingleTableOperation) {
            if (returnedLastInsertId != null && returnedLastInsertId != 0) {
                executionContext.getConnection().setReturnedLastInsertId(returnedLastInsertId);
            }
            if (lastInsertId != null && lastInsertId != 0) {
                // Override the value set by MyJdbcHandler
                executionContext.getConnection().setLastInsertId(lastInsertId);
            } else if (((SingleTableOperation) logicalPlan).getAutoIncParamIndex()
                != SingleTableOperation.NO_AUTO_INC) {
                // Using sequence, but the value is explicitly assigned, recover
                // last insert id to the original one.
                executionContext.getConnection().setLastInsertId(oldLastInsertId);
            }
        }

        if (executionContext.useReturning()) {
            return modifyCursor;
        } else {
            return new AffectRowCursor(affectRows);
        }
    }

    /**
     * For INSERT, sequence should be calculated before sharding. The procedure
     * is a little different from ReplaceSequenceWithLiteralVisitor.
     */
    private Long[] handleWithSequence(RelNode logicalPlan, ExecutionContext executionContext) {
        if (!(logicalPlan instanceof SingleTableOperation)) {
            return null;
        }

        SingleTableOperation operation = (SingleTableOperation) logicalPlan;
        int autoIncParamIndex = operation.getAutoIncParamIndex();
        // If autoIncParamIndex != -1, it must be INSERT or REPLACE
        if (autoIncParamIndex == SingleTableOperation.NO_AUTO_INC) {
            return null;
        }

        Parameters parameters = executionContext.getParams();
        Map<Integer, ParameterContext> param = parameters.getCurrentParameter();
        if (param == null) {
            return null;
        }

        String schemaName = operation.getSchemaName();
        if (StringUtils.isEmpty(schemaName)) {
            schemaName = executionContext.getSchemaName();
        }
        String tableName = operation.getLogicalTableNames().get(0);

        // Sequence calculation is too complex: we need to record lastInsertId
        // and returnedLastInsertId, consider SPLIT mode, and update sequence
        // start value, etc. So we just reuse this visitor.
        ReplaceSequenceWithLiteralVisitor visitor = new ReplaceSequenceWithLiteralVisitor(parameters,
            -1,
            schemaName,
            tableName,
            SequenceAttribute.getAutoValueOnZero(executionContext.getSqlMode()));
        visitor.replaceDynamicParam(autoIncParamIndex);
        return new Long[] {visitor.getLastInsertId(), visitor.getReturnedLastInsertId()};
    }

    protected void logAllPhyDmlSqlIfNeed(ExecutionContext executionContext, RelNode logicalPlan, int[] affectRows) {

        if (!executionContext.getParamManager().getBoolean(ConnectionParams.ENABLE_SCALE_OUT_ALL_PHY_DML_LOG)
            && !((BaseTableOperation) logicalPlan).isReplicateRelNode()) {
            return;
        }

        Map<Integer, ParameterContext> params =
            executionContext.getParams() == null ? null : executionContext.getParams().getCurrentParameter();
        logScaleOutSql(executionContext.getTraceId(), affectRows[0], affectRows[0], "--",
            (BaseTableOperation) logicalPlan,
            ((BaseQueryOperation) logicalPlan).getDbIndexAndParam(params, executionContext), executionContext);
    }

    protected void logScaleOutSql(String traceId,
                                  int srcAffectedRows,
                                  int targetAffectedRows,
                                  String targetPhyGroup,
                                  BaseTableOperation targetPhyPlan,
                                  Pair<String, Map<Integer, ParameterContext>> dbIndexAndParamOfPhyOperation,
                                  ExecutionContext executionContext) {

        if (!executionContext.getParamManager().getBoolean(ConnectionParams.ENABLE_SCALE_OUT_GROUP_PHY_DML_LOG)) {
            return;
        }

        Object tbNames = null;
        if (targetPhyPlan instanceof SingleTableOperation) {
            tbNames = ((SingleTableOperation) targetPhyPlan).getLogicalTableNames().get(0);
        } else {
            tbNames = targetPhyPlan.getTableNames().get(0);
        }

        String tbName = "";
        if (tbNames instanceof List) {
            tbName = (String) ((List) tbNames).get(0);
        } else if (tbNames instanceof String) {
            tbName = (String) tbNames;
        }
        String sourcePhyGroup = dbIndexAndParamOfPhyOperation.getKey();

        String targetSql = targetPhyPlan.getNativeSql();
        String formatTargetSql = LogFormat.formatLog(targetSql);

        Map<Integer, ParameterContext> params = dbIndexAndParamOfPhyOperation.getValue();

        JSONArray jsonArray = new JSONArray();
        if (params != null && params.size() > 0) {
            for (Map.Entry<Integer, ParameterContext> pair : params.entrySet()) {
                jsonArray.add(pair.getValue().getValue());
            }
        } else if (targetPhyPlan.getBatchParameters() != null) {
            for (Map<Integer, ParameterContext> item : targetPhyPlan.getBatchParameters()) {
                for (Map.Entry<Integer, ParameterContext> pair : item.entrySet()) {
                    jsonArray.add(pair.getValue().getValue());
                }
            }
        }
        boolean replicateRelNode = targetPhyPlan.isReplicateRelNode();
        String paramsString = jsonArray.toJSONString();
        String scaleOutSqlLog = String
            .format(
                "[traceId:%s] [src_ar/tar_ar:%d/%d] [src/tar:%s/%s] [tb:%s] [isReplicateRel:%s/%s] [params:%s], [ExecutionContext:%s], [batchParameter:%s]",
                traceId,
                srcAffectedRows, targetAffectedRows, sourcePhyGroup, targetPhyGroup, tbName,
                String.valueOf(replicateRelNode),
                formatTargetSql,
                paramsString,
                executionContext.toString(),
                targetPhyPlan.getBatchParameters() != null ?
                    String.valueOf(System.identityHashCode((targetPhyPlan).getBatchParameters())) :
                    "");
        scaleOutSqlLogger.info(scaleOutSqlLog);

    }

}
