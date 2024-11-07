package com.alibaba.polardbx.executor.ddl.job.task.gsi;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.eventlogger.EventType;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.corrector.Checker;
import com.alibaba.polardbx.executor.corrector.CheckerCallback;
import com.alibaba.polardbx.executor.corrector.Reporter;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.ddl.job.task.BaseBackfillTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.fastchecker.FastChecker;
import com.alibaba.polardbx.executor.gsi.CheckerManager;
import com.alibaba.polardbx.executor.gsi.GsiUtils;
import com.alibaba.polardbx.executor.gsi.corrector.Corrector;
import com.alibaba.polardbx.executor.gsi.corrector.GsiChecker;
import com.alibaba.polardbx.executor.gsi.corrector.GsiReporter;
import com.alibaba.polardbx.executor.gsi.fastchecker.GsiFastChecker;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalCheckGsi;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.CheckGsiPrepareData;
import com.alibaba.polardbx.optimizer.utils.QueryConcurrencyPolicy;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import com.google.common.collect.ImmutableMap;
import lombok.Getter;
import org.apache.calcite.sql.SqlSelect;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static com.alibaba.polardbx.executor.utils.ExecUtils.getQueryConcurrencyPolicy;

/**
 * Check consistency of global index
 *
 * @author moyi
 * @since 2021/07
 */
@Getter
@TaskName(name = "SperateCheckGsiTask")
public class SperateCheckGsiTask extends CheckGsiTask {
    public static SperateCheckGsiTask create(CheckGsiPrepareData prepareData) {
        return new SperateCheckGsiTask(
            prepareData.getSchemaName(),
            prepareData.getTableName(),
            prepareData.getIndexName(),
            prepareData.getLockMode().getKey().name(),
            prepareData.getLockMode().getValue().name(),
            new GsiChecker.Params(
                prepareData.getBatchSize(),
                prepareData.getSpeedLimit(),
                prepareData.getSpeedMin(),
                prepareData.getParallelism(),
                prepareData.getEarlyFailNumber(),
                prepareData.isUseBinary()
            ),
            prepareData.isCorrect(),
            prepareData.getExtraCmd(),
            false,
            false,
            null,
            null
        );
    }

    @JSONCreator
    public SperateCheckGsiTask(String schemaName,
                               String tableName,
                               String indexName,
                               String primaryTableLockMode,
                               String indexTableLockMode,
                               GsiChecker.Params checkParams,
                               boolean correct,
                               String extraCmd,
                               boolean primaryBroadCast,
                               boolean gsiBroadCast,
                               Map<String, String> virtualColumnMap,
                               Map<String, String> backfillColumnMap) {
        super(schemaName, tableName,
            indexName, primaryTableLockMode,
            indexTableLockMode, checkParams,
            correct, extraCmd, primaryBroadCast,
            gsiBroadCast, false);
    }

    @Override
    protected void executeImpl(ExecutionContext ec) {
        checkInBackfill(ec);
    }
}
