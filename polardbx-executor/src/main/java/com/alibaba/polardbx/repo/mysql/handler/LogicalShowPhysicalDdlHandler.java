package com.alibaba.polardbx.repo.mysql.handler;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.polardbx.atom.TAtomDataSource;
import com.alibaba.polardbx.common.model.Group;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.ddl.twophase.TwoPhaseDdlUtils;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import com.alibaba.polardbx.repo.mysql.spi.MyRepository;
import com.alibaba.polardbx.rpc.compatible.XDataSource;
import io.grpc.netty.shaded.io.netty.util.internal.StringUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlShowPhysicalDdl;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author jinkun.taojinkun
 */
public class LogicalShowPhysicalDdlHandler extends HandlerCommon {

    private static final Logger logger = LoggerFactory.getLogger(LogicalShowPhysicalDdlHandler.class);

    public LogicalShowPhysicalDdlHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        final LogicalShow show = (LogicalShow) logicalPlan;
        final SqlShowPhysicalDdl showPhysicalDdl = (SqlShowPhysicalDdl) show.getNativeSqlNode();

        String schemaName = showPhysicalDdl.getSchema();
        if (DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
            return handle(executionContext, schemaName, showPhysicalDdl.isStatus(), true);
        } else {
            return handle(executionContext, schemaName, showPhysicalDdl.isStatus(), false);
        }
    }

    public class PhysicalDdlResult {
        public String getPhysicalDbName() {
            return physicalDbName;
        }

        public String getPhysicalTableName() {
            return physicalTableName;
        }

        public Long getProcessId() {
            return processId;
        }

        public String getPhase() {
            return phase;
        }

        public String getState() {
            return state;
        }

        String key;
        String physicalDbName;

        String physicalTableName;

        Long processId;

        String phase;

        String state;

        String processInfo = "";

        String processState = "";

        Long time = -1L;

        String reachedPreparedMoment = "";

        String reachedCommitMoment = "";

        String commitMoment = "";

        String prepareMoment = "";

        Long preparedRunningConsumeBlocks = -1L;

        Long preparedRunningConsumeTime = -1L;

        Long commitConsumeBlocks = -1L;

        Long commitConsumeTime = -1L;

        Long lockTableTime = -1L;

        public void setProcessInfo(String processInfo) {
            this.processInfo = processInfo;
        }

        public void setProcessState(String processState) {
            this.processState = processState;
        }

        public void setTime(Long time) {
            this.time = time;
        }

        public PhysicalDdlResult(String key, String physicalDbName, String physicalTableName, Long processId,
                                 String phase,
                                 String state
        ) {
            this.key = key;
            this.physicalDbName = physicalDbName;
            this.physicalTableName = physicalTableName;
            this.processId = processId;
            this.phase = phase;
            this.state = state;
        }

        public PhysicalDdlResult(String physicalTableName, String reachedPreparedMoment, String reachedCommitMoment,
                                 String commitMoment, String prepareMoment,
                                 Long preparedRunningConsumeBlocks, Long preparedRunningConsumeTime,
                                 Long commitConsumeBlocks,
                                 Long commitConsumeTime, Long lockTableTime) {
            this.physicalTableName = physicalTableName;
            this.reachedPreparedMoment = reachedPreparedMoment;
            this.reachedCommitMoment = reachedCommitMoment;
            this.commitMoment = commitMoment;
            this.prepareMoment = prepareMoment;
            this.preparedRunningConsumeBlocks = preparedRunningConsumeBlocks;
            this.preparedRunningConsumeTime = preparedRunningConsumeTime;
            this.commitConsumeBlocks = commitConsumeBlocks;
            this.commitConsumeTime = commitConsumeTime;
            this.lockTableTime = lockTableTime;
        }

        public void assignProfInfo(PhysicalDdlResult physicalDdlResult) {
            this.reachedPreparedMoment = physicalDdlResult.reachedPreparedMoment;
            this.reachedCommitMoment = physicalDdlResult.reachedCommitMoment;
            this.commitMoment = physicalDdlResult.commitMoment;
            this.prepareMoment = physicalDdlResult.prepareMoment;
            this.preparedRunningConsumeBlocks = physicalDdlResult.preparedRunningConsumeBlocks;
            this.preparedRunningConsumeTime = physicalDdlResult.preparedRunningConsumeTime;
            this.commitConsumeBlocks = physicalDdlResult.commitConsumeBlocks;
            this.commitConsumeTime = physicalDdlResult.commitConsumeTime;
            this.lockTableTime = physicalDdlResult.lockTableTime;
        }

    }

    private Cursor handle(ExecutionContext executionContext, String schemaName, Boolean status, Boolean newPartition) {
        ArrayResultCursor result = getShowPhysicalDdlResultCursor();
        Throwable ex = null;
        MyRepository repo = (MyRepository) this.repo;
        List<Group> allGroups = OptimizerContext.getActiveGroups();
        Map<String, PhysicalDdlResult> physicalDdlResultMap = new HashMap<>();
        List<PhysicalDdlResult> physicalDdlResults = new ArrayList<>();
        String showFullPhysicalDdlStatsSql = TwoPhaseDdlUtils.SQL_STATS_FULL_TWO_PHASE_DDL;
        String showFullPhysicalDdlProfSql = TwoPhaseDdlUtils.SQL_PROF_FULL_TWO_PHASE_DDL;
        String showPhysicalProcessSql = TwoPhaseDdlUtils.SQL_SHOW_PROCESS_LIST;
        Set<Pair<String, String>> visitedDnSet = new HashSet<>();
        for (Group group : allGroups) {
            if (!group.getType().equals(Group.GroupType.MYSQL_JDBC)) {
                continue;
            }

            TGroupDataSource groupDataSource = (TGroupDataSource) repo.getDataSource(group.getName());
            if (groupDataSource == null) {
                continue;
            }
            TAtomDataSource atom = TwoPhaseDdlUtils.findMasterAtomForGroup(groupDataSource);
            if (!visitedDnSet.add(new Pair<>(atom.getHost(), atom.getPort()))) {
                // 防止不同的库在相同的实例上导致的重复
                continue;
            }
            final DataSource dataSource = atom.getDataSource();
            Connection conn = null;

            try {
                if (dataSource instanceof DruidDataSource) {
                    DruidDataSource druid = (DruidDataSource) dataSource;
                    conn = druid.createPhysicalConnection().getPhysicalConnection();
                } else if (dataSource instanceof XDataSource) {
                    conn = dataSource.getConnection();
                } else {
                    throw GeneralUtil.nestedException("Unknown datasource. " + dataSource.getClass());
                }
                Map<String, PhysicalDdlResult> physicalDdlProfInfoMap = new HashMap<>();
                ResultSet rsPhysicalDdlProf = conn.createStatement().executeQuery(showFullPhysicalDdlProfSql);
                while (rsPhysicalDdlProf.next()) {
                    String physicalTableName = rsPhysicalDdlProf.getString("PHYSICAL_TABLE");
                    String reachedPreparedMoment = rsPhysicalDdlProf.getString("REACHED_PREPARED_MOMENT");
                    String reachedCommitMoment = rsPhysicalDdlProf.getString("REACHED_COMMIT_MOMENT");
                    String commitMoment = rsPhysicalDdlProf.getString("COMMIT_MOMENT");
                    String prepareMoment = rsPhysicalDdlProf.getString("PREPARE_MOMENT");
                    Long preparedRunningConsumeBlocks = rsPhysicalDdlProf.getLong("PREPARED_RUNNING_CONSUME_BLOCKS");
                    Long preparedRunningConsumeTime = rsPhysicalDdlProf.getLong("PREPARED_RUNNING_CONSUME_TIME");
                    Long commitConsumeBlocks = rsPhysicalDdlProf.getLong("COMMIT_CONSUME_BLOCKS");
                    Long commitConsumeTime = rsPhysicalDdlProf.getLong("COMMIT_CONSUME_TIME");
                    Long lockTableTime = rsPhysicalDdlProf.getLong("LOCK_TABLE_TIME");
                    PhysicalDdlResult physicalDdlResult = new PhysicalDdlResult(
                        physicalTableName,
                        reachedPreparedMoment,
                        reachedCommitMoment,
                        commitMoment,
                        prepareMoment,
                        preparedRunningConsumeBlocks,
                        preparedRunningConsumeTime,
                        commitConsumeBlocks,
                        commitConsumeTime,
                        lockTableTime
                    );
                    physicalDdlProfInfoMap.put(physicalTableName, physicalDdlResult);
                }
                ResultSet rsPhysicalDdlStats = conn.createStatement().executeQuery(showFullPhysicalDdlStatsSql);
                while (rsPhysicalDdlStats.next()) {
                    String key = rsPhysicalDdlStats.getString("KEY");
                    String phyDbName = rsPhysicalDdlStats.getString("PHYSICAL_DB");
                    String physicalTableName = rsPhysicalDdlStats.getString("PHYSICAL_TABLE");
                    String phase = rsPhysicalDdlStats.getString("PHASE");
                    String state = rsPhysicalDdlStats.getString("STATE");
                    Long processId = rsPhysicalDdlStats.getLong("PROCESS_ID");
                    PhysicalDdlResult physicalDdlResult = new PhysicalDdlResult(
                        key,
                        phyDbName,
                        physicalTableName,
                        processId,
                        phase,
                        state
                    );
                    if (physicalDdlProfInfoMap.containsKey(physicalTableName)) {
                        physicalDdlResult.assignProfInfo(physicalDdlProfInfoMap.get(physicalTableName));
                    }
                    if (processId == -1L) {
                        physicalDdlResults.add(physicalDdlResult);
                    } else {
                        physicalDdlResultMap.put(buildDdlResultKey(phyDbName, processId), physicalDdlResult);
                    }
                }

                ResultSet rsPhysicalProcess = conn.createStatement().executeQuery(showPhysicalProcessSql);
                while (rsPhysicalProcess.next()) {
                    Long processId = rsPhysicalProcess.getLong("Id");
                    String phyDbName = rsPhysicalProcess.getString("db");
                    String ddlResultKey = buildDdlResultKey(phyDbName, processId);
                    if (physicalDdlResultMap.containsKey(ddlResultKey)) {
                        String processState = rsPhysicalProcess.getString("State");
                        String processInfo = rsPhysicalProcess.getString("Info");
                        Long time = rsPhysicalProcess.getLong("Time");
                        PhysicalDdlResult physicalDdlResult = physicalDdlResultMap.get(ddlResultKey);
                        physicalDdlResult.setProcessInfo(processInfo);
                        physicalDdlResult.setProcessState(processState);
                        physicalDdlResult.setTime(time);

                    }
                }
                conn.close();
                conn = null;
            } catch (SQLException e) {
                logger.error("error when show physical ddl", e);
            } finally {
                if (conn != null) {
                    try {
                        conn.close();
                    } catch (SQLException e) {
                        logger.error(e);
                    }
                }
            }
        }
        physicalDdlResults.addAll(physicalDdlResultMap.values());
        Set<String> groupNames = DbTopologyManager.getGroupNameToStorageInstIdMap(schemaName).keySet();
        Set<String> physicalDbNames =
            groupNames.stream().map(GroupInfoUtil::buildPhysicalDbNameFromGroupName).collect(Collectors.toSet());
        physicalDdlResults =
            physicalDdlResults.stream().filter(o -> physicalDbNames.contains(o.physicalDbName)).collect(
                Collectors.toList());

        if (!status) {
            outputPhysicalDdlResult(result, physicalDdlResults);
            return result;
        } else {
            return handleShowPhysicalDdlStatus(physicalDdlResults);
        }
    }

    class ClusterPhyDbStatus {
        String phase;
        String phyDbName;
        int totalCount;
        Map<String, Integer> stateCount;

        String logicalTableName;

        ClusterPhyDbStatus(String phyDbName, String logicalTableName, String phase) {
            this.phase = phase;
            this.phyDbName = phyDbName;
            this.stateCount = new HashMap<>();
            this.logicalTableName = logicalTableName;
            this.totalCount = 0;
        }

        void appendResult(PhysicalDdlResult physicalDdlResult) {
            this.totalCount += 1;
            if (!this.stateCount.containsKey(physicalDdlResult.state)) {
                this.stateCount.put(physicalDdlResult.state, 0);
            }
            this.stateCount.put(physicalDdlResult.state, this.stateCount.get(physicalDdlResult.state) + 1);
        }

        @Override
        public String toString() {
            String totalCountString = String.format("total Physical Table Count: %d, ", totalCount);
            List<String> stateCountStrings = new ArrayList<>();
            for (String state : stateCount.keySet()) {
                int count = stateCount.get(state);
                stateCountStrings.add(String.format("%s Count:%d", state, count));
            }
            return totalCountString + StringUtil.join(", ", stateCountStrings);
        }
    }

    public Cursor handleShowPhysicalDdlStatus(List<PhysicalDdlResult> physicalDdlResults) {
        ArrayResultCursor result = getShowPhysicalDdlStatusResultCursor();

        Map<String, ClusterPhyDbStatus> phyDbStatus = new HashMap<>();
        for (PhysicalDdlResult physicalDdlResult : physicalDdlResults) {
            if (!phyDbStatus.containsKey(physicalDdlResult.key)) {
                String logicalTableName =
                    TwoPhaseDdlUtils.buildLogicalTableNameFromTwoPhaseKeyAndPhyDbName(physicalDdlResult.key,
                        physicalDdlResult.physicalDbName);
                phyDbStatus.put(physicalDdlResult.key,
                    new ClusterPhyDbStatus(physicalDdlResult.physicalDbName, logicalTableName,
                        physicalDdlResult.phase));
            }
            phyDbStatus.get(physicalDdlResult.key).appendResult(physicalDdlResult);
        }
        for (String key : phyDbStatus.keySet()) {
            ClusterPhyDbStatus clusterPhyDbStatus = phyDbStatus.get(key);
            result.addRow(new Object[] {
                clusterPhyDbStatus.logicalTableName,
                clusterPhyDbStatus.phyDbName,
                clusterPhyDbStatus.phase,
                clusterPhyDbStatus.toString()
            });
        }
        return result;

    }

//        String targetSql1 = TwoPhaseDdlUtils.SQL_STATS_FULL_TWO_PHASE_DDL;
//        String targetSql2 = "show processlist";
//        Map<String, PhysicalDdlResult> physicalDdlResults = new HashMap<>();
//        for(String phyDbName:phyDbNames){
//            String groupName = buildGroupNameFromPhysicalDb(phyDbName);
//            TGroupDataSource tGroupDataSource = (TGroupDataSource) ExecutorContext.getContext(schemaName).getTopologyExecutor()
//                .getGroupExecutor(groupName).getDataSource();
//            try(Connection conn = (Connection)tGroupDataSource.getConnection()){
//                ResultSet rs1 = conn.createStatement().executeQuery(targetSql1);
//                ResultSet rs2 = conn.createStatement().executeQuery(targetSql2);
//                while(rs1.next()) {
//                    String physicalTableName = rs1.getString("PHYSICAL_TABLE");
//                    String phase = rs2.getString("PHASE");
//                    String state = rs2.getString("WAIT_RUNNING");
//                    Long processId = rs2.getLong("PROCESS_ID");
//                    PhysicalDdlResult physicalDdlResult = new PhysicalDdlResult(
//                        phyDbName,
//                        physicalTableName,
//                        processId,
//                        phase,
//                        state
//                    );
//                    physicalDdlResults.put(getDdlResultKey(phyDbName, processId), physicalDdlResult);
//                }
//                while(rs2.next()) {
//                    Long processId = rs2.getLong("ID");
//                    String ddlResultKey = getDdlResultKey(phyDbName, processId);
//                    if(physicalDdlResults.containsKey(ddlResultKey)) {
//                        String processState = rs2.getString("State");
//                        String processInfo = rs2.getString("Info");
//                        Long time = rs2.getLong("Time");
//                        PhysicalDdlResult physicalDdlResult = physicalDdlResults.get(ddlResultKey);
//                        physicalDdlResult.setProcessInfo(processInfo);
//                        physicalDdlResult.setProcessState(processState);
//                        physicalDdlResult.setTime(time);
//                    }
//                }
//            }catch(Throwable e){
//                logger.error(e);
//                ex = e;
//            } finally {
//                if (ex != null) {
//                    GeneralUtil.nestedException(ex);
//                }
//            }
//        }

    public void outputPhysicalDdlResult(ArrayResultCursor result, Collection<PhysicalDdlResult> physicalDdlResults) {
        List<PhysicalDdlResult> sortedPhysicalDdlResults =
            physicalDdlResults.stream().sorted(Comparator.comparing(PhysicalDdlResult::getPhysicalDbName)
                .thenComparing(PhysicalDdlResult::getPhase)
                .thenComparing(PhysicalDdlResult::getState)
                .thenComparing(PhysicalDdlResult::getPhysicalTableName)
            ).collect(Collectors.toList());
        for (PhysicalDdlResult physicalDdlResult : sortedPhysicalDdlResults) {
            result.addRow(new Object[] {
                physicalDdlResult.physicalDbName,
                physicalDdlResult.physicalTableName,
                physicalDdlResult.phase,
                physicalDdlResult.state,
                physicalDdlResult.processId,
                physicalDdlResult.processState,
                physicalDdlResult.time,

                physicalDdlResult.reachedPreparedMoment,
                physicalDdlResult.reachedCommitMoment,
                physicalDdlResult.commitMoment,
                physicalDdlResult.prepareMoment,
                physicalDdlResult.preparedRunningConsumeBlocks,
                physicalDdlResult.preparedRunningConsumeTime,
                physicalDdlResult.commitConsumeBlocks,
                physicalDdlResult.commitConsumeTime,
                physicalDdlResult.lockTableTime
            });
        }
    }

    private ArrayResultCursor getShowPhysicalDdlStatusResultCursor() {
        ArrayResultCursor result = new ArrayResultCursor("PHYSICAL_DDL_STATUS");
        result.addColumn("LOGICAL_TABLE_NAME", DataTypes.StringType);
        result.addColumn("PHYSICAL_DB_NAME", DataTypes.StringType);
        result.addColumn("PHASE", DataTypes.StringType);
        result.addColumn("CONTENT", DataTypes.StringType);
        return result;
    }

    private ArrayResultCursor getShowPhysicalDdlResultCursor() {
        ArrayResultCursor result = new ArrayResultCursor("PHYSICAL_DDL_INFO");
        result.addColumn("PHYSICAL_DB_NAME", DataTypes.StringType);
        result.addColumn("PHYSICAL_TABLE_NAME", DataTypes.StringType);
        result.addColumn("PHASE", DataTypes.StringType);
        result.addColumn("STATE", DataTypes.StringType);
        result.addColumn("PROCESS_ID", DataTypes.LongType);
        result.addColumn("PROCESS_STATE", DataTypes.StringType);
        result.addColumn("TIME", DataTypes.LongType);

        result.addColumn("REACHED_PREPARED_MOMENT", DataTypes.StringType);
        result.addColumn("REACHED_COMMIT_MOMENT", DataTypes.StringType);
        result.addColumn("COMMIT_MOMENT", DataTypes.StringType);
        result.addColumn("PREPARE_MOMENT", DataTypes.StringType);
        result.addColumn("PREPARED_RUNNING_CONSUME_BLOCKS", DataTypes.LongType);
        result.addColumn("PREPARED_RUNNING_CONSUME_TIME", DataTypes.LongType);
        result.addColumn("COMMIT_CONSUME_BLOCKS", DataTypes.LongType);
        result.addColumn("COMMIT_CONSUME_TIME", DataTypes.LongType);
        result.addColumn("LOCK_TABLE_TIME", DataTypes.LongType);
//        result.addColumn("RUN_TIME_SECOND", DataTypes.LongType);
//        result.addColumn("WAIT_ON_BARRIER", DataTypes.StringType);
//        result.addColumn("WAIT_TIME", DataTypes.LongType);
//        result.addColumn("PHYSICAL_TABLE_SIZE", DataTypes.LongType);
//        result.addColumn("PHYSICAL_TABLE_ROWS", DataTypes.LongType);
//        result.addColumn("PHYSICAL_TABLE_LINE", DataTypes.LongType);
//        result.addColumn("PHYSICAL_TABLE_ROW_LOG_SIZE", DataTypes.LongType);
//        result.addColumn("PHYSICAL_TABLE_ROW_LOG_REPLAY_TIME", DataTypes.LongType);
//        result.addColumn("COPY_TMP_TABLE_TIME", DataTypes.LongType);
//        result.addColumn("REPLAY_ROW_LOG_TIME", DataTypes.LongType);
//        result.addColumn("COMMIT_TIME", DataTypes.LongType);
//        result.addColumn("PREPARE_TIME", DataTypes.LongType);
//        result.addColumn("WAIT_TIME", DataTypes.LongType);
//        result.addColumn("DDL_STMT", DataTypes.StringType);
        result.initMeta();
        return result;
    }

    private String buildDdlResultKey(String physicalDbName, Long processId) {
        return String.format("%s_%d", physicalDbName, processId);
    }
}
