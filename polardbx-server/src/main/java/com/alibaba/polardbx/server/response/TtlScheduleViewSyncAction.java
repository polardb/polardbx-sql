package com.alibaba.polardbx.server.response;

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.scheduler.TtlScheduledJobStatManager;
import com.alibaba.polardbx.executor.handler.subhandler.InformationSchemaTtlScheduleHandler;
import com.alibaba.polardbx.executor.mpp.deploy.ServiceProvider;
import com.alibaba.polardbx.executor.scheduler.ScheduledJobsManager;
import com.alibaba.polardbx.executor.sync.ISyncAction;
import com.alibaba.polardbx.gms.node.InternalNodeManager;
import com.alibaba.polardbx.gms.node.Node;
import com.alibaba.polardbx.gms.scheduler.ScheduleDateTimeConverter;
import com.alibaba.polardbx.gms.scheduler.ScheduledJobExecutorType;
import com.alibaba.polardbx.gms.scheduler.ScheduledJobsRecord;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

import java.util.ArrayList;
import java.util.List;

public class TtlScheduleViewSyncAction implements ISyncAction {

    protected String schema;

    public TtlScheduleViewSyncAction() {
    }

    public TtlScheduleViewSyncAction(String schema) {
        this.schema = schema;
    }

    @Override
    public ResultCursor sync() {

        ArrayResultCursor result = new ArrayResultCursor("TTL_SCHEDULE");
        result.addColumn("TABLE_SCHEMA", DataTypes.VarcharType);
        result.addColumn("TABLE_NAME", DataTypes.VarcharType);
        result.addColumn("METRIC_KEY", DataTypes.VarcharType);
        result.addColumn("METRIC_VAL", DataTypes.VarcharType);

        result.initMeta();

        InternalNodeManager nodeManager = ServiceProvider.getInstance().getServer().getNodeManager();
        Node node = nodeManager.getCurrentNode();
        if (!node.isLeader()) {
            return result;
        }

        List<ScheduledJobsRecord> recordList = ScheduledJobsManager.queryScheduledJobsRecord();
        for (ScheduledJobsRecord rs : recordList) {

            if (!ScheduledJobExecutorType.TTL_JOB.name().equalsIgnoreCase(rs.getExecutorType())) {
                continue;
            }

            List<Pair<String, String>> metricInfos = new ArrayList<>();
            metricInfos.add(new Pair<>("SCHEDULE_STATUS", String.valueOf(rs.getStatus())));
            metricInfos.add(new Pair<>("SCHEDULE_EXPR", String.valueOf(rs.getScheduleExpr())));
            metricInfos.add(new Pair<>("SCHEDULE_COMMENT", String.valueOf(rs.getScheduleComment())));
            metricInfos.add(new Pair<>("SCHEDULE_TIMEZONE", String.valueOf(rs.getTimeZone())));
            metricInfos.add(new Pair<>("SCHEDULE_LAST_FIRE_TIME",
                ScheduleDateTimeConverter.secondToZonedDateTime(rs.getLastFireTime(), rs.getTimeZone())
                    .toLocalDateTime().format(InformationSchemaTtlScheduleHandler.ISO_DATETIME_FORMATTER)));
            metricInfos.add(new Pair<>("SCHEDULE_NEXT_FIRE_TIME",
                ScheduleDateTimeConverter.secondToZonedDateTime(rs.getNextFireTime(), rs.getTimeZone())
                    .toLocalDateTime().format(InformationSchemaTtlScheduleHandler.ISO_DATETIME_FORMATTER)));

            String dbName = rs.getTableSchema();
            String tbName = rs.getTableName();
            TtlScheduledJobStatManager.TtlJobStatInfo jobStatInfo =
                TtlScheduledJobStatManager.getInstance().getTtlJobStatInfo(dbName, tbName);
            if (jobStatInfo != null) {
                metricInfos.addAll(jobStatInfo.toMetricInfo());
            }

            for (int i = 0; i < metricInfos.size(); i++) {
                result.addRow(new Object[] {
                    dbName,
                    tbName,
                    metricInfos.get(i).getKey(),
                    metricInfos.get(i).getValue()
                });
            }
        }

        List<Pair<String, String>> globalMetricInfos =
            TtlScheduledJobStatManager.getInstance().getGlobalTtlJobStatInfo()
                .toMetricInfo();
        String globalDbName = "global";
        String globalTbName = "global";
        for (int i = 0; i < globalMetricInfos.size(); i++) {
            result.addRow(new Object[] {
                globalDbName,
                globalTbName,
                globalMetricInfos.get(i).getKey(),
                globalMetricInfos.get(i).getValue()
            });
        }
        return result;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }
}
