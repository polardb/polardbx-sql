package com.alibaba.polardbx.executor.handler.subhandler;

import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.scheduler.TtlScheduledJobStatManager;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.executor.scheduler.ScheduledJobsManager;
import com.alibaba.polardbx.executor.sync.ISyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.scheduler.ScheduleDateTimeConverter;
import com.alibaba.polardbx.gms.scheduler.ScheduledJobExecutorType;
import com.alibaba.polardbx.gms.scheduler.ScheduledJobsRecord;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.scalar.CanAccessTable;
import com.alibaba.polardbx.optimizer.view.InformationSchemaTtlSchedule;
import com.alibaba.polardbx.optimizer.view.VirtualView;

import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by guxu.
 *
 * @author guxu
 */
public class InformationSchemaTtlScheduleHandler extends BaseVirtualViewSubClassHandler {

    public static final DateTimeFormatter ISO_DATETIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private static Class ttlScheduleViewSyncActionClass;

    static {
        // 只有server支持，这里是暂时改法，后续要将这段逻辑解耦
        try {
            ttlScheduleViewSyncActionClass =
                Class.forName("com.alibaba.polardbx.server.response.TtlScheduleViewSyncAction");
        } catch (ClassNotFoundException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, e, e.getMessage());
        }
    }

    public InformationSchemaTtlScheduleHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaTtlSchedule;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {

        InformationSchemaTtlSchedule ttlScheduleView = (InformationSchemaTtlSchedule) virtualView;

        ISyncAction ttlScheduleSyncAction;
        if (ttlScheduleViewSyncActionClass == null) {
            throw new NotSupportException();
        }
        try {
            ttlScheduleSyncAction =
                (ISyncAction) ttlScheduleViewSyncActionClass.getConstructor().newInstance();
        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, e, e.getMessage());
        }

        List<List<Map<String, Object>>> resultsOfAllNodes =
            SyncManagerHelper.sync(ttlScheduleSyncAction, SyncScope.MASTER_ONLY);

        if (resultsOfAllNodes == null) {
            return cursor;
        }

        for (int i = 0; i < resultsOfAllNodes.size(); i++) {
            List<Map<String, Object>> resultsOfOneNode = resultsOfAllNodes.get(i);
            if (resultsOfOneNode == null) {
                continue;
            }
            for (int j = 0; j < resultsOfOneNode.size(); j++) {
                Map<String, Object> oneRow = resultsOfOneNode.get(j);
                String tableSchema = (String) oneRow.get("TABLE_SCHEMA");
                String tableName = (String) oneRow.get("TABLE_NAME");
                String metricKey = (String) oneRow.get("METRIC_KEY");
                String metricVal = (String) oneRow.get("METRIC_VAL");
                cursor.addRow(new Object[] {
                    tableSchema,
                    tableName,
                    metricKey,
                    metricVal
                });
            }
        }

        return cursor;
    }
}

