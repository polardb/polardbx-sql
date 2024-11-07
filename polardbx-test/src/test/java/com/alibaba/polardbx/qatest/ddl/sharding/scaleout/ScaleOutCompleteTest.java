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

package com.alibaba.polardbx.qatest.ddl.sharding.scaleout;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.common.utils.thread.ExecutorUtil;
import com.alibaba.polardbx.common.utils.thread.NamedThreadFactory;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableList;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.yaml.snakeyaml.Yaml;

import net.jcip.annotations.NotThreadSafe;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author chenghui.lch
 */

@NotThreadSafe
public class ScaleOutCompleteTest extends ScaleOutBaseTest {

    private static List<ComplexTaskMetaManager.ComplexTaskStatus> moveTableStatus =
        Stream.of(ComplexTaskMetaManager.ComplexTaskStatus.WRITE_ONLY).collect(Collectors.toList());

    protected int taskExecutorPoolSize = 16;
    protected ThreadPoolExecutor scaleOutTaskExecutor =
        ExecutorUtil.createExecutor("scaleOutTestTaskExecutor", 1);

    protected ThreadPoolExecutor scaleOutWorkLoadTaskExecutor =
        new ThreadPoolExecutor(taskExecutorPoolSize,
            taskExecutorPoolSize,
            1800,
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<Runnable>(100), // without buffer
            new NamedThreadFactory("scaleOutTestTaskExecutor", true),
            new ThreadPoolExecutor.AbortPolicy());

    protected static List<String> workloadNameList = new ArrayList<>();
    protected static final String DATA_KEY = "data";

    static {
        //workloadNameList.add("add_data");
//        workloadNameList.add("stb_update_delete_limit1");
//        workloadNameList.add("stb_update_delete_limit2");
//        workloadNameList.add("bro_dml1");
//        workloadNameList.add("bro_dml2");
//        workloadNameList.add("bro_simple_insert");
//        workloadNameList.add("bro_replace");
//        workloadNameList.add("stb_insert_replace1");
//        workloadNameList.add("stb_insert_replace2");
        workloadNameList.add("stb_update_delete1");
        workloadNameList.add("stb_update_delete2");
    }

    public ScaleOutCompleteTest(StatusInfo tableStatus) {
        super("ScaleOutCompleteTest", "polardbx_meta_db_polardbx",
            ImmutableList.of(tableStatus.getMoveTableStatus().toString()), false);
    }

    @Before
    public void setUpTables() {
    }

    @After
    public void tearDown() {
    }

    @Parameterized.Parameters(name = "{index}:TableStatus={0}")
    public static List<StatusInfo[]> prepareDate() {
        List<StatusInfo[]> status = new ArrayList<>();
        moveTableStatus.stream().forEach(c -> {
            status.add(new StatusInfo[] {new StatusInfo(c, false)});
        });
        return status;
    }

    protected static class ScaleOutTaskContext {
        public Connection polarxConn;
        public Connection polarxScaleOutConn;
        public Connection metaDbConn;

        public String logDbForTest = "ScaleOutCompleteTest";
        public String metaDb = "polardbx_meta_db_polardbx";

        public List<String> groupNames = new ArrayList<>();
        public List<String> storageInstIdList = new ArrayList<>();

        public Map<String, List<String>> storageGroupsInfo = new HashMap<>();
        public Map<String, String> groupToStorageIdMap = new HashMap<>();
        public Map<String, String> grpAndPhyDbMap = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        public Map<String, String> srcGrpAndTmpGrpMap = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);

        public Map<String, String> workloadDdls = new HashMap<>();
        public Map<String, String> workloadDatas = new HashMap<>();

        public String groupToBeMoved = null;
        public String targetStorageInstId = null;
        public String scaleOutTaskSql = null;

        public Future scaleOutFuture = null;
        public volatile boolean doScaleOutSucc = true;

        public volatile boolean isStopWorkloads = false;

        public ScaleOutCompleteTest testParent = null;

        public Map<String, WorkloadInfo> workloadSqlInfosMap = new HashMap<>();
        public List<Future> workloadFutures = new ArrayList<>();

        public int logTbCnt = 0;
        public int concurrentCnt = 8;
        public long waitTimeEachDb = 120000;
        //public long waitTimeEachDb = 60000;
        public long totalWaitTime = 0;
        public long workloadTime = 0;
        public String scaleOutHint =
            String.format(
                "/*+TDDL:CMD_EXTRA(SHARE_STORAGE_MODE=true, PHYSICAL_BACKFILL_ENABLE=false, SCALE_OUT_DROP_DATABASE_AFTER_SWITCH_DATASOURCE=true, SCALE_OUT_DEBUG_WAIT_TIME_IN_WO=%s,SCALEOUT_BACKFILL_BATCH_SIZE=2, SCALEOUT_BACKFILL_SPEED_LIMITATION=32)*/",
                String.valueOf(waitTimeEachDb));

        public ScaleOutTaskContext() {
        }

        public static class WorkloadInfo {

            public String dataSql = "";
            public Map<String, String> sqlInfos = new HashMap<>();
            public List<Integer> sqlIdList = new ArrayList<>();

            public WorkloadInfo() {
            }
        }
    }

    @Test
    public void testScaleOutTask() {

        if (usingNewPartDb()) {
            return;
        }

        ScaleOutTaskContext context = null;

        try {
            // Check and prepare scale out env
            context = checkAndPrepareEnv();

            for (int i = 0; i < context.groupNames.size(); i++) {
                String tarGrp = context.groupNames.get(i);
                context.groupToBeMoved = tarGrp;
                doScaleOutTaskAndCheckForOneGroup(context);
            }
            // Remove data of Unused group

        } catch (Throwable ex) {
            ex.printStackTrace();
            Assert.fail(ex.getMessage());
        } finally {
            // Check and clear scale out env
            checkAndClearEnv(context);
        }
    }

    private void doScaleOutTaskAndCheckForOneGroup(ScaleOutTaskContext context) {
        // Start ScaleOut Task
        startScaleOutTask(context);

        // Start workloads
        startWorkloads(context);

        // Stop workloads
        stopWorkloads(context);

        // Wait ScaleOut Task to finish
        waitScaleOutTaskFinish(context);

        // Check sum table For target datas
        checkSumTable(context);

        // reset
        resetContext(context);
    }

    protected static class ScaleOutWorkloadTask implements Runnable {

        protected ScaleOutTaskContext context;
        protected ScaleOutCompleteTest testParent;

        public ScaleOutWorkloadTask(ScaleOutTaskContext context) {
            this.context = context;
            this.testParent = context.testParent;
        }

        @Override
        public void run() {
            if (context.isStopWorkloads) {
                return;
            }
            ScaleOutTaskContext.WorkloadInfo workloadSqlInfo = prepare();
            runTask(workloadSqlInfo);
        }

        protected ScaleOutTaskContext.WorkloadInfo prepare() {

            Random random = new Random();

            Set<String> workloadInfoNameSet = context.workloadSqlInfosMap.keySet();
            List<String> nameList = new ArrayList<>();
            nameList.addAll(workloadInfoNameSet);

            int workLoadTaskIdx = Math.abs(random.nextInt() % nameList.size());

            String workloadInfoName = nameList.get(workLoadTaskIdx);
            ScaleOutTaskContext.WorkloadInfo workloadSqlInfo = context.workloadSqlInfosMap.get(workloadInfoName);
            return workloadSqlInfo;
        }

        protected void runTask(ScaleOutTaskContext.WorkloadInfo workloadSqlInfo) {
            runSqlByTrans(workloadSqlInfo);
            try {
                Thread.sleep(500);
            } catch (Throwable ex) {
                // ignore ex
            }
            resubmitWorkloadTaskForNext();
        }

        protected void runSqlByTrans(ScaleOutTaskContext.WorkloadInfo workloadSqlInfo) {
            try (Connection conn = ConnectionManager.getInstance().getDruidPolardbxConnection()) {

                String useDbSql = String.format("use %s;", context.logDbForTest);
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute(useDbSql);
                } catch (Throwable ex) {
                    ex.printStackTrace();
                }

                conn.setAutoCommit(false);

                Map<String, String> sqlInfos = workloadSqlInfo.sqlInfos;
                List<Integer> sqlIdList = workloadSqlInfo.sqlIdList;
                String dataSql = workloadSqlInfo.dataSql;

                if (dataSql != null) {
                    try (Statement stmt = conn.createStatement()) {
                        stmt.execute(dataSql);
                    } catch (Throwable ex) {
                        ex.printStackTrace();
                    }
                }

                for (int i = 0; i < sqlIdList.size(); i++) {
                    String sql = sqlInfos.get(String.valueOf(sqlIdList.get(i)));
                    try (Statement stmt = conn.createStatement()) {
                        stmt.execute(sql);
                    } catch (Throwable ex) {
                        String msg = ex.getMessage();
                        if (!msg.contains("in readonly mode") && !msg.contains("ERR_TRANS_CONTINUE_AFTER_WRITE_FAIL")) {
                            ex.printStackTrace();
                        }
                        break;
                    }
                }
                conn.commit();
            } catch (Throwable ex) {
                String msg = ex.getMessage();
                if (!msg.contains("in readonly mode") && !msg.contains("ERR_TRANS_CONTINUE_AFTER_WRITE_FAIL")) {
                    ex.printStackTrace();
                }
            }
        }

        protected void resubmitWorkloadTaskForNext() {
            try {
                this.testParent.scaleOutWorkLoadTaskExecutor.submit(new ScaleOutWorkloadTask(this.context));
            } catch (Throwable ex) {
                // ignore
            }

        }
    }

    protected ScaleOutTaskContext checkAndPrepareEnv() {
        ScaleOutTaskContext scaleOutTaskContext = new ScaleOutTaskContext();
        scaleOutTaskContext.testParent = this;
        scaleOutTaskContext.polarxConn = tddlConnection;
        scaleOutTaskContext.metaDbConn = mysqlConnection;
        reCreateDatabase(scaleOutTaskContext.polarxConn, scaleOutTaskContext.logDbForTest,
            scaleOutTaskContext.metaDbConn, scaleOutTaskContext.metaDb, 1);
        initStorageGroupInit(scaleOutTaskContext);
        prepareTables(scaleOutTaskContext);
        prepareDatas(scaleOutTaskContext);
        loadWorkloadInfos(scaleOutTaskContext);
        scaleOutTaskContext.logTbCnt = scaleOutTaskContext.workloadDdls.size();
        scaleOutTaskContext.totalWaitTime = scaleOutTaskContext.waitTimeEachDb;
        scaleOutTaskContext.workloadTime = scaleOutTaskContext.totalWaitTime * 3 / 5;
        return scaleOutTaskContext;
    }

    protected void prepareTables(ScaleOutTaskContext context) {
        loadWorkloadDdls(context);
        Map<String, String> workloadDdls = context.workloadDdls;
        try {
            Connection conn = context.polarxConn;
            for (Map.Entry<String, String> tbAndDdl : workloadDdls.entrySet()) {
                String ddl = tbAndDdl.getValue();
                JdbcUtil.executeUpdate(conn, ddl);
            }
        } catch (Throwable ex) {
            ex.printStackTrace();
        }
    }

    protected void prepareDatas(ScaleOutTaskContext context) {
        loadWorkloadDatas(context);
        Map<String, String> workloadDatas = context.workloadDatas;
        try {
            Connection conn = context.polarxConn;
            for (Map.Entry<String, String> tbAndSql : workloadDatas.entrySet()) {
                String datas = tbAndSql.getValue();
                JdbcUtil.executeUpdate(conn, datas);
            }
        } catch (Throwable ex) {
            ex.printStackTrace();
        }
    }

    protected void initStorageGroupInit(ScaleOutTaskContext context) {

        String logicalDatabase = context.logDbForTest;
        Map<String, List<String>> storageGroupsInfo = context.storageGroupsInfo;
        List<String> groupNames = context.groupNames;
        Connection tddlConnection = context.polarxConn;
        Map<String, String> groupToStorageIdMap = context.groupToStorageIdMap;

        String tddlSql = "use " + logicalDatabase;
        JdbcUtil.executeUpdate(tddlConnection, tddlSql);
        tddlSql = "show datasources";
        storageGroupsInfo.clear();
        try {
            PreparedStatement stmt = JdbcUtil.preparedStatement(tddlSql, tddlConnection);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                String sid = rs.getString("STORAGE_INST_ID");
                String groupName = rs.getString("GROUP");
                int writeWeight = rs.getInt("WRITE_WEIGHT");
                if (writeWeight <= 0 || groupName.toUpperCase().endsWith("_SINGLE_GROUP") || groupName
                    .equalsIgnoreCase("MetaDB")) {
                    continue;
                }
                groupNames.add(groupName);
                if (!storageGroupsInfo.containsKey(sid)) {
                    storageGroupsInfo.put(sid, new ArrayList<>());
                }
                storageGroupsInfo.get(sid).add(groupName);
                groupToStorageIdMap.put(groupName, sid);
            }
            rs.close();
            stmt.close();
        } catch (Throwable e) {
            e.printStackTrace();
        }
        storageGroupsInfo.forEach((k, v) -> context.storageInstIdList.add(k));

        Collections.sort(context.groupNames);
        Collections.sort(context.storageInstIdList);
    }

    protected void resetContext(ScaleOutTaskContext scaleOutTaskContext) {
        scaleOutTaskContext.scaleOutFuture = null;
        scaleOutTaskContext.workloadFutures.clear();
        scaleOutTaskContext.isStopWorkloads = false;
        scaleOutTaskContext.doScaleOutSucc = true;
    }

    protected void checkAndClearEnv(ScaleOutTaskContext scaleOutTaskContext) {

        scaleOutTaskContext.polarxConn = tddlConnection;
        scaleOutTaskContext.metaDbConn = mysqlConnection;

        JdbcUtil.executeUpdate(tddlConnection, "use information_schema");
        String tddlSql =
            "/*+TDDL:cmd_extra(ALLOW_DROP_DATABASE_IN_SCALEOUT_PHASE=true)*/drop database if exists "
                + scaleOutTaskContext.logDbForTest;
        JdbcUtil.executeUpdate(getTddlConnection1(), tddlSql);
    }

    protected void startScaleOutTask(ScaleOutTaskContext context) {

        if (context.groupNames.isEmpty()) {
            throw new RuntimeException(
                String.format("no find a new storage for group[%s] to move", context.groupToBeMoved));
        }

        // Prepare scale out task sql
        if (context.groupToBeMoved == null) {
            context.groupToBeMoved = context.groupNames.get(0);
        }

        Map<String, String> grpToStorageIdMap = context.groupToStorageIdMap;
        String storageIdOfGroup = grpToStorageIdMap.get(context.groupToBeMoved);

        Map<String, String> targetGroupAndTargetSidMap = new HashMap<>();
        boolean findTargetSid = false;
        String targetStorageId = "";
        for (int i = 0; i < context.storageInstIdList.size(); i++) {
            String sid = context.storageInstIdList.get(i);
            if (storageIdOfGroup.equalsIgnoreCase(sid)) {
                continue;
            }
            findTargetSid = true;
            targetStorageId = sid;
            context.targetStorageInstId = sid;
            targetGroupAndTargetSidMap.put(context.groupToBeMoved, targetStorageId);
            break;
        }
        if (!findTargetSid) {
            throw new RuntimeException(
                String.format("no find a new storage for group[%s] to move", context.groupToBeMoved));
        }

        context.scaleOutTaskSql = String
            .format("move database %s %s to '%s';", context.scaleOutHint, context.groupToBeMoved,
                context.targetStorageInstId);

        context.polarxScaleOutConn = tddlConnection;
        Future future = scaleOutTaskExecutor.submit(new Runnable() {
            @Override
            public void run() {

                try {
                    Connection conn = context.polarxScaleOutConn;
                    String scaleOutSql = context.scaleOutTaskSql;
                    String useDbSql = String.format("use %s;", context.logDbForTest);
                    try (Statement stmt = conn.createStatement()) {
                        stmt.execute(useDbSql);
                    } catch (Throwable ex) {
                        ex.printStackTrace();
                    }

                    try (Statement stmt = conn.createStatement()) {
                        stmt.execute(scaleOutSql);
                    } catch (Throwable ex) {
                        ex.printStackTrace();
                    }

                    try (Statement stmt = conn.createStatement()) {
                        stmt.execute("show move database;");
                        ResultSet rs = stmt.getResultSet();
                        String grpToMoved = context.groupToBeMoved;
                        while (rs.next()) {
                            String grpName = rs.getString("SOURCE_DB_GROUP_KEY");
                            String jobStatus = rs.getString("JOB_STATUS");
                            if (grpName.equalsIgnoreCase(grpToMoved)) {
                                if (jobStatus.equalsIgnoreCase("FAILED")) {
                                    context.doScaleOutSucc = false;
                                    break;
                                }
                            }
                        }
                    } catch (Throwable ex) {
                        ex.printStackTrace();
                    }
                } catch (Throwable ex) {
                    ex.printStackTrace();
                }
            }
        });

        context.scaleOutFuture = future;
        System.out.print(context.scaleOutTaskSql);

    }

    protected void startWorkloads(ScaleOutTaskContext context) {

        for (int i = 0; i < context.concurrentCnt; i++) {
            ScaleOutWorkloadTask workloadTask = new ScaleOutWorkloadTask(context);
            scaleOutTaskExecutor.submit(workloadTask);
        }

        try {
            long sleepTime = context.workloadTime / 1000;
            for (int i = 0; i < sleepTime; i++) {
                if (context.doScaleOutSucc) {
                    Thread.sleep(1000);
                } else {
                    break;
                }
            }
        } catch (Throwable ex) {
            ex.printStackTrace();
        }

    }

    protected void stopWorkloads(ScaleOutTaskContext context) {

        context.isStopWorkloads = true;
        ThreadPoolExecutor executor = context.testParent.scaleOutWorkLoadTaskExecutor;
        while (true) {
            if (executor.getActiveCount() == 0 && executor.getQueue().size() == 0) {
                break;
            } else {
                try {
                    Thread.sleep(500);
                } catch (Throwable ex) {
                    // ignore
                }
            }
        }

        System.out.print("succ to stop workloads.");
    }

    protected void waitScaleOutTaskFinish(ScaleOutTaskContext context) {
        while (true) {
            if (!context.scaleOutFuture.isDone()) {
                try {
                    Thread.sleep(1000);
                } catch (Throwable ex) {
                    ex.printStackTrace();
                }
            } else {
                break;
            }
        }

    }

    protected void checkSumTable(ScaleOutTaskContext context) {

        try {
            if (!context.doScaleOutSucc) {
                Assert.fail("Failed to move database because the job has been failed");
                return;
            }

            Connection conn = context.polarxConn;
            String showDs = String.format("show ds;", context.logDbForTest);
            Map<String, String> grpAndPhyDbMap = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(showDs);
                ResultSet rs = stmt.getResultSet();
                while (rs.next()) {
                    String grp = rs.getString("GROUP");
                    String phyDb = rs.getString("PHY_DB");
                    grpAndPhyDbMap.put(grp, phyDb);
                }
                context.grpAndPhyDbMap = grpAndPhyDbMap;
                rs.close();
            } catch (Throwable ex) {
                ex.printStackTrace();
            }
            String useLogDb = String.format("use %s;", context.logDbForTest);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(useLogDb);
            } catch (Throwable ex) {
                ex.printStackTrace();
            }
            String curMoveGroup = context.groupToBeMoved;
            String showMoveDatabase = String.format("show move database;");
            Map<String, String> srcGrpAndTmpGrpMap = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(showMoveDatabase);
                ResultSet rs = stmt.getResultSet();
                while (rs.next()) {
                    String srcGrp = rs.getString("SOURCE_DB_GROUP_KEY");
                    String tmpGrp = rs.getString("TEMPORARY_DB_GROUP_KEY");
                    String jobStatus = rs.getString("JOB_STATUS");
                    if (!jobStatus.equalsIgnoreCase("TO_BE_CLEAN")) {
                        continue;
                    }
                    srcGrpAndTmpGrpMap.put(srcGrp, tmpGrp);
                }
                context.srcGrpAndTmpGrpMap = srcGrpAndTmpGrpMap;
                rs.close();
            } catch (Throwable ex) {
                ex.printStackTrace();
            }

            Connection metaDbConn = context.metaDbConn;
            for (Map.Entry<String, String> srcAndTmpItem : srcGrpAndTmpGrpMap.entrySet()) {
                String srcGrp = srcAndTmpItem.getKey();
                String tmpGrp = srcAndTmpItem.getValue();
                String srcDb = grpAndPhyDbMap.get(srcGrp);
                String tmpDb = grpAndPhyDbMap.get(tmpGrp);
                if (!curMoveGroup.equalsIgnoreCase(srcGrp)) {
                    continue;
                }
                checkSumTableForScaleOutGroup(srcDb, tmpDb, metaDbConn);
            }
        } catch (Throwable ex) {
            throw ex;
        }
    }

    protected void checkSumTableForScaleOutGroup(String srcDb, String tmpDb, Connection conn) {

        try {
            List<String> tableListOfSrcDb = new ArrayList<>();
            List<String> tableListOfTmpDb = new ArrayList<>();
            List<String> tableList = new ArrayList<>();
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(String.format("show tables from %s;", srcDb));
                ResultSet rs = stmt.getResultSet();
                while (rs.next()) {
                    String tb = rs.getString(1);
                    if (tb.equalsIgnoreCase("__drds_global_tx_log") || tb.equalsIgnoreCase("__drds_redo_log")) {
                        continue;
                    }
                    tableList.add(tb);
                    tableListOfSrcDb.add(String.format("%s.%s", srcDb, tb));
                    tableListOfTmpDb.add(String.format("%s.%s", tmpDb, tb));
                }
                rs.close();
            } catch (Throwable ex) {
                ex.printStackTrace();
            }

            String checkSumTableSqlOfScrDb =
                String.format("checksum table %s", StringUtils.join(tableListOfSrcDb, ","));
            String checkSumTableSqlOfTmpDb =
                String.format("checksum table %s", StringUtils.join(tableListOfTmpDb, ","));
            Map<String, String> checkSumTableRsOfSrcDb = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
            Map<String, String> checkSumTableRsOfTmpDb = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(String.format(checkSumTableSqlOfScrDb));
                ResultSet rs = stmt.getResultSet();
                while (rs.next()) {
                    String tbName = rs.getString(1);
                    String checkSum = rs.getString(2);
                    checkSumTableRsOfSrcDb.put(tbName, checkSum);
                }
                rs.close();
            } catch (Throwable ex) {
                ex.printStackTrace();
            }
            try (Statement stmt = conn.createStatement()) {
                stmt.execute(String.format(checkSumTableSqlOfTmpDb));
                ResultSet rs = stmt.getResultSet();
                while (rs.next()) {
                    String tbName = rs.getString(1);
                    String checkSum = rs.getString(2);
                    checkSumTableRsOfTmpDb.put(tbName, checkSum);
                }
                rs.close();
            } catch (Throwable ex) {
                ex.printStackTrace();
            }

            assert checkSumTableRsOfSrcDb.size() == checkSumTableRsOfTmpDb.size();
            boolean checkSumOk = true;
            for (String tb : tableList) {
                String srcTb = String.format("%s.%s", srcDb, tb);
                String tmpTb = String.format("%s.%s", tmpDb, tb);
                String checkSumRsOfSrc = checkSumTableRsOfSrcDb.get(srcTb);
                String checkSumRsOfTmp = checkSumTableRsOfTmpDb.get(tmpTb);
                if (!checkSumRsOfSrc.equals(checkSumRsOfTmp)) {
                    checkSumOk = false;
                    break;
                }
            }
            Assert.assertTrue(checkSumOk, "check sum table fail after scaleout");
        } catch (Throwable ex) {
            throw ex;
        }
    }

    private void waitTaskFinish(Future future) {
        while (true) {
            if (future != null && !future.isDone()) {
                try {
                    Thread.sleep(1000);
                } catch (Throwable ex) {
                    ex.printStackTrace();
                }
            } else {
                break;
            }
        }
    }

    private void loadWorkloadDdls(ScaleOutTaskContext context) {
        String fileName = "scaleout.env/workloads.ddl.yml";
        context.workloadDdls = loadInfoFromYml(fileName);
    }

    private void loadWorkloadDatas(ScaleOutTaskContext context) {
        String fileName = "scaleout.env/workloads.data.yml";
        context.workloadDatas = loadInfoFromYml(fileName);
    }

    private void loadWorkloadInfos(ScaleOutTaskContext context) {
        Map<String, ScaleOutTaskContext.WorkloadInfo> workloadInfosMap = new HashMap<>();
        for (int i = 0; i < workloadNameList.size(); i++) {
            String workloadName = workloadNameList.get(i);
            String workloadFileName = String.format("scaleout.env/%s.yml", workloadName);
            Map<String, String> sqlInfos = loadInfoFromYml(workloadFileName);
            ScaleOutTaskContext.WorkloadInfo workloadInfo = new ScaleOutTaskContext.WorkloadInfo();

            String dataSql = sqlInfos.get(DATA_KEY);
            List<Integer> sqlIds = new ArrayList<>();
            for (String key : sqlInfos.keySet()) {
                if (key.equalsIgnoreCase(DATA_KEY)) {
                    continue;
                }
                sqlIds.add(Integer.valueOf(key));
            }
            Collections.sort(sqlIds);
            workloadInfo.sqlInfos = sqlInfos;
            workloadInfo.sqlIdList = sqlIds;
            workloadInfo.dataSql = dataSql;
            workloadInfosMap.put(workloadName, workloadInfo);
        }
        context.workloadSqlInfosMap = workloadInfosMap;
    }

    public Map<String, String> loadInfoFromYml(String fileName) {
        InputStream in = this.getClass().getClassLoader().getResourceAsStream(fileName);
        Yaml yaml = new Yaml();
        Map<String, String> infos = yaml.loadAs(in, Map.class);
        Map<String, String> finalInfos = new HashMap<>();
        for (Object key : infos.keySet()) {
            finalInfos.put(String.valueOf(key), infos.get(key));
        }
        IOUtils.closeQuietly(in);
        return finalInfos;
    }
}
