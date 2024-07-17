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

import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.eventlogger.EventType;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.executor.backfill.Loader;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.fastchecker.FastChecker;
import com.alibaba.polardbx.executor.gsi.BackfillExecutor;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.partitionmanagement.fastchecker.LogicalTableDataMigrationFastChecker;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.LogicalTableDataMigrationBackfill;
import com.alibaba.polardbx.optimizer.utils.PhyTableOperationUtil;
import com.alibaba.polardbx.optimizer.utils.QueryConcurrencyPolicy;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import org.apache.calcite.rel.RelNode;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

import static com.alibaba.polardbx.executor.utils.ExecUtils.getQueryConcurrencyPolicy;

/**
 * Created by zhuqiwei.
 */
public class LogicalTableDataMigrationBackfillHandler extends HandlerCommon {

    public LogicalTableDataMigrationBackfillHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext ec) {
        LogicalTableDataMigrationBackfill backfill = (LogicalTableDataMigrationBackfill) logicalPlan;
        String srcSchemaName = backfill.getSrcSchemaName();
        String dstSchemaName = backfill.getDstSchemaName();
        String srcLogicalTable = backfill.getSrcTableName();
        String dstLogicalTable = backfill.getDstTableName();

        BackfillExecutor backfillExecutor =
            new BackfillExecutor((List<RelNode> inputs, ExecutionContext executionContext) -> {
                QueryConcurrencyPolicy queryConcurrencyPolicy = getQueryConcurrencyPolicy(executionContext);
                if (Loader.canUseBackfillReturning(executionContext, dstSchemaName)) {
                    queryConcurrencyPolicy = QueryConcurrencyPolicy.GROUP_CONCURRENT_BLOCK;
                }
                List<Cursor> inputCursors = new ArrayList<>(inputs.size());
                executeWithConcurrentPolicy(executionContext, inputs, queryConcurrencyPolicy, inputCursors,
                    dstSchemaName);
                return inputCursors;
            });

        ec = clearSqlMode(ec.copy());

        if (!ec.getParamManager().getBoolean(ConnectionParams.BACKFILL_USING_BINARY)) {
            upgradeEncoding(ec, srcSchemaName, srcLogicalTable);
        }

        PhyTableOperationUtil.disableIntraGroupParallelism(dstSchemaName, ec);

        // Force master first and following will copy this EC.
        ec.getExtraCmds().put(ConnectionProperties.MASTER, true);

        List<String> dstLogicalGsiNames = getTableGsiNames(dstSchemaName, dstLogicalTable);
        int affectRows =
            backfillExecutor.logicalTableDataMigrationBackFill(srcSchemaName, dstSchemaName, srcLogicalTable,
                dstLogicalTable, dstLogicalGsiNames, ec);

        // Check target table immediately after backfill by default.
        boolean doCheck = ec.getParamManager().getBoolean(ConnectionParams.CREATE_DATABASE_AS_USE_FASTCHECKER);
        if (doCheck) {
            //check primary table
            boolean fastCheckSucc = fastcheck(srcSchemaName, dstSchemaName, srcLogicalTable, dstLogicalTable, ec);
            if (!fastCheckSucc) {
                throw new TddlRuntimeException(ErrorCode.ERR_FAST_CHECKER, "table data is inconsistent");
            }
            //check Gsi table
            for (String gsi : dstLogicalGsiNames) {
                if (!fastcheck(srcSchemaName, dstSchemaName, srcLogicalTable, gsi, ec)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_FAST_CHECKER, "table data is inconsistent");
                }
            }
        }

        return new AffectRowCursor(affectRows);
    }

    protected boolean fastcheck(String schemaSrc, String schemaDst, String logicalTableSrc, String logicalTableDst,
                                ExecutionContext ec) {
        long startTime = System.currentTimeMillis();
        SQLRecorderLogger.ddlLogger.info(MessageFormat.format(
            "FastChecker for table data migration, srcSchema [{0}] dstSchema [{1}] logical table src [{2}] logical table dst [{3}]start",
            schemaSrc, schemaDst, logicalTableSrc, logicalTableDst));

        List<FastChecker> fastCheckers =
            LogicalTableDataMigrationFastChecker.create(schemaSrc, schemaDst, logicalTableSrc, logicalTableDst, ec);
        if (fastCheckers == null || fastCheckers.isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, "failed to create table data migration fastchecker");
        }

        boolean checkResult = false;
        try {
            boolean singleResult = true;
            for (FastChecker fastChecker : fastCheckers) {
                singleResult = fastChecker.check(ec);
                if (singleResult == false) {
                    break;
                }
            }
            checkResult = singleResult;
        } catch (TddlNestableRuntimeException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_SCALEOUT_EXECUTE, e,
                "table data migration fastchecker failed to check");
        } finally {
            SQLRecorderLogger.ddlLogger.info(MessageFormat.format(
                "FastChecker for table data migration, srcSchema [{0}] dstSchema [{1}] logical tableSrc [{2}] logical tableDst [{3}] finish, time use [{4}], check result [{5}]",
                schemaSrc, schemaDst, logicalTableSrc, logicalTableDst,
                (System.currentTimeMillis() - startTime) / 1000.0, checkResult ? "pass" : "not pass"));
            if (!checkResult) {
                EventLogger.log(EventType.DDL_WARN, "FastChecker failed");
            } else {
                EventLogger.log(EventType.DDL_INFO, "FastChecker succeed");
            }
        }

        return checkResult;
    }

    protected List<String> getTableGsiNames(String schemaName, String primaryTableName) {
        ExecutorContext ec = ExecutorContext.getContext(schemaName);
        List<String> allGsiNames = new ArrayList<>();
        if (null == ec) {
            throw new TddlRuntimeException(ErrorCode.ERR_UNKNOWN_DATABASE, schemaName);
        }
        GsiMetaManager metaManager = ec.getGsiManager().getGsiMetaManager();
        GsiMetaManager.GsiMetaBean meta =
            metaManager.getTableAndIndexMeta(primaryTableName, EnumSet.of(IndexStatus.PUBLIC));

        for (GsiMetaManager.GsiTableMetaBean bean : meta.getTableMeta().values()) {
            if (bean.gsiMetaBean != null && !bean.gsiMetaBean.columnarIndex) {
                GsiMetaManager.GsiIndexMetaBean bean1 = bean.gsiMetaBean;
                allGsiNames.add(bean1.indexName);
            }
        }
        return allGsiNames;
    }

}
