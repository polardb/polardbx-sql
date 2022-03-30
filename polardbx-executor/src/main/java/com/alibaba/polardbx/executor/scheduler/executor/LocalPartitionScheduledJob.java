package com.alibaba.polardbx.executor.scheduler.executor;

import com.alibaba.polardbx.common.model.Group;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.timezone.InternalTimeZone;
import com.alibaba.polardbx.common.utils.timezone.TimeZoneUtils;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.partitionmanagement.LocalPartitionManager;
import com.alibaba.polardbx.executor.scheduler.ScheduledJobsManager;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.scheduler.ExecutableScheduledJob;
import com.alibaba.polardbx.optimizer.config.server.DefaultServerConfigManager;
import com.alibaba.polardbx.optimizer.config.server.IServerConfigManager;
import com.alibaba.polardbx.optimizer.utils.OptimizerHelper;
import com.alibaba.polardbx.repo.mysql.checktable.LocalPartitionDescription;
import com.alibaba.polardbx.repo.mysql.checktable.TableDescription;
import com.alibaba.polardbx.repo.mysql.spi.MyRepository;
import com.google.common.base.Joiner;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.FAILED;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.QUEUED;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.RUNNING;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.SUCCESS;

/**
 * 1. allocate new local partitions
 * 2. expire old local partitions
 *
 * @author guxu
 */
public class LocalPartitionScheduledJob implements SchedulerExecutor{

    private static final Logger logger = LoggerFactory.getLogger(LocalPartitionScheduledJob.class);

    private final ExecutableScheduledJob executableScheduledJob;

    public LocalPartitionScheduledJob(final ExecutableScheduledJob executableScheduledJob) {
        this.executableScheduledJob = executableScheduledJob;
    }

    @Override
    public boolean execute() {
        String tableSchema = executableScheduledJob.getTableSchema();
        String timeZoneStr = executableScheduledJob.getTimeZone();
        long scheduleId = executableScheduledJob.getScheduleId();
        long fireTime = executableScheduledJob.getFireTime();
        long startTime = ZonedDateTime.now().toEpochSecond();
        try {
            //mark as RUNNING
            boolean casSuccess = ScheduledJobsManager.casStateWithStartTime(scheduleId, fireTime, QUEUED, RUNNING, startTime);
            if(!casSuccess){
                return false;
            }
            //execute
            FailPoint.injectException("FP_LOCAL_PARTITION_SCHEDULED_JOB_ERROR");

            final String tableName = executableScheduledJob.getTableName();
            final InternalTimeZone timeZone = TimeZoneUtils.convertFromMySqlTZ(timeZoneStr);

            IRepository repository = ExecutorContext.getContext(tableSchema).getTopologyHandler()
                .getRepositoryHolder().get(Group.GroupType.MYSQL_JDBC.toString());
            List<LocalPartitionDescription> preLocalPartitionList =
                getLocalPartitionList((MyRepository) repository, tableSchema, tableName);

            logger.info(String.format("start allocating local partition. table:[%s]", tableName));
            executeBackgroundSql(String.format("ALTER TABLE %s ALLOCATE LOCAL PARTITION", tableName), tableSchema, timeZone);
            logger.info(String.format("start expiring local partition. table:[%s]", tableName));
            executeBackgroundSql(String.format("ALTER TABLE %s EXPIRE LOCAL PARTITION", tableName), tableSchema, timeZone);

            List<LocalPartitionDescription> postLocalPartitionList =
                getLocalPartitionList((MyRepository) repository, tableSchema, tableName);

            String remark = genRemark(preLocalPartitionList, postLocalPartitionList);

            //mark as SUCCESS
            long finishTime = ZonedDateTime.now().toEpochSecond();
            casSuccess = ScheduledJobsManager.casStateWithFinishTime(scheduleId, fireTime, RUNNING, SUCCESS, finishTime, remark);
            if(!casSuccess){
                return false;
            }
            return true;
        }catch (Throwable t){
            logger.error(String.format(
                "process scheduled local partition job:[%s] error, fireTime:[%s]", scheduleId, fireTime), t);
            ScheduledJobsManager.updateState(scheduleId, fireTime, FAILED, null, t.getMessage());
            return false;
        }
    }

    private void executeBackgroundSql(String sql, String schemaName, InternalTimeZone timeZone) {
        IServerConfigManager serverConfigManager = getServerConfigManager();
        serverConfigManager.executeBackgroundSql(sql, schemaName, timeZone);
    }

    private IServerConfigManager getServerConfigManager() {
        IServerConfigManager serverConfigManager = OptimizerHelper.getServerConfigManager();
        if (serverConfigManager == null) {
            serverConfigManager = new DefaultServerConfigManager(null);
        }
        return serverConfigManager;
    }

    private List<LocalPartitionDescription> getLocalPartitionList(MyRepository repository,
                                                                  String tableSchema,
                                                                  String tableName){
        List<TableDescription> tableDescriptionList = LocalPartitionManager.getLocalPartitionInfoList(
            repository, tableSchema, tableName, true);
        TableDescription tableDescription = tableDescriptionList.get(0);
        List<LocalPartitionDescription> localPartitionDescriptionList = tableDescription.getPartitions();
        return localPartitionDescriptionList;
    }

    private String genRemark(List<LocalPartitionDescription> pre, List<LocalPartitionDescription> post){
        String remark = "";
        if(CollectionUtils.isEmpty(pre) || CollectionUtils.isEmpty(post)){
            return remark;
        }
        try {
            Set<String> preDesc = pre.stream().map(e->e.getPartitionDescription()).collect(Collectors.toSet());
            Set<String> postDesc = post.stream().map(e->e.getPartitionDescription()).collect(Collectors.toSet());
            Set<String> allocated = Sets.difference(postDesc, preDesc);
            Set<String> expired = Sets.difference(preDesc, postDesc);
            if(CollectionUtils.isNotEmpty(allocated)){
                remark += "allocated:" + Joiner.on(",").join(allocated) + ";";
            }
            if(CollectionUtils.isNotEmpty(expired)){
                remark += "expired:" + Joiner.on(",").join(expired) + ";";
            }
            return remark;
        }catch (Exception e){
            return remark;
        }
    }
}