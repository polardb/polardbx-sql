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

import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.qatest.constant.ScaleOutConstant;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.linq4j.Ord;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import net.jcip.annotations.NotThreadSafe;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.MessageFormat;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.alibaba.polardbx.qatest.constant.TableConstant.C_INT_32;
import static com.alibaba.polardbx.qatest.constant.TableConstant.FULL_TYPE_TABLE_COLUMNS;
import static com.alibaba.polardbx.qatest.constant.TableConstant.FULL_TYPE_TABLE_COLUMN_DEFS;
import static com.alibaba.polardbx.qatest.data.ExecuteTableSelect.PRIMARY_KEY_TEMPLATE;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
@RunWith(Parameterized.class)
@NotThreadSafe
public class ScaleOutDMLFullDataTypeTest extends ScaleOutBaseTest {

    private static PrepareTable normalTable1;
    private static PrepareTable normalTable2;
    private final String indexSk;
    private final ExecutorService dmlPool = Executors.newFixedThreadPool(4);
    private static List<ComplexTaskMetaManager.ComplexTaskStatus> moveTableStatus =
        Stream.of(ComplexTaskMetaManager.ComplexTaskStatus.CREATING,
            ComplexTaskMetaManager.ComplexTaskStatus.DELETE_ONLY,
            ComplexTaskMetaManager.ComplexTaskStatus.WRITE_ONLY,
            //ScaleOutMetaManager.MoveTableStatus.WRITE_REORG,
            ComplexTaskMetaManager.ComplexTaskStatus.READY_TO_PUBLIC,
            ComplexTaskMetaManager.ComplexTaskStatus.PUBLIC).collect(Collectors.toList());
    final ComplexTaskMetaManager.ComplexTaskStatus finalTableStatus;
    static boolean firstIn = true;
    static ComplexTaskMetaManager.ComplexTaskStatus currentStatus = ComplexTaskMetaManager.ComplexTaskStatus.CREATING;
    final Boolean isCache;
    String[] allLogicalTables =
        new String[] {PRIMARY_TABLE_NAME1, PRIMARY_TABLE_NAME2};
    private static Map<String, List<String>> physicalTablesOnSourceGroup = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    public ScaleOutDMLFullDataTypeTest(StatueInfoExt tableStatus) {
        super("ScaleOutDMLFullDataTypeTest", "polardbx_meta_db_polardbx",
            ImmutableList.of(tableStatus.getMoveTableStatus().toString()));
        String index = tableStatus.getIndexSK();
        if (index.toLowerCase().indexOf("int") == -1 && index.toLowerCase().indexOf("point") != -1) {
            index = C_INT_32;
        }
        this.indexSk = index;
        finalTableStatus = tableStatus.getMoveTableStatus();
        isCache = tableStatus.isCache();
        if (!currentStatus.equals(finalTableStatus)) {
            firstIn = true;
            currentStatus = finalTableStatus;
        }
    }

    @Parameterized.Parameters(name = "{index}:TableStatus={0}")
    public static List<StatueInfoExt[]> prepareDate() {
        List<StatueInfoExt[]> status = new ArrayList<>();
        moveTableStatus.stream().forEach(c -> {
            FULL_TYPE_TABLE_COLUMNS.stream()
                .filter(c1 -> c1.toLowerCase().indexOf("int") != -1 && c1.toLowerCase().indexOf("point") == -1)
                .forEach(f -> {
                    status.add(new StatueInfoExt[] {new StatueInfoExt(c, false, f)});
                    status.add(new StatueInfoExt[] {new StatueInfoExt(c, true, f)});
                    status.add(new StatueInfoExt[] {new StatueInfoExt(c, null, f)});
                });

        });
        return status;
    }

    @Before
    public void before() {
        if (firstIn) {
            normalTable1 = new PrepareTable(indexSk, false, 1);
            normalTable2 = new PrepareTable(indexSk, false, 2);
            setUp(true, getTableDefs(), true);
            firstIn = false;
            for (String logicalTable : allLogicalTables) {
                if (logicalTable.toLowerCase().startsWith("brd_")) {
                    physicalTablesOnSourceGroup.put(logicalTable, ImmutableList.of(logicalTable));
                } else {
                    physicalTablesOnSourceGroup
                        .put(logicalTable, getAllPhysicalTables(new String[] {logicalTable}).get(sourceGroupKey));
                }
            }
        }
        String tddlSql = "use " + logicalDatabase;
        JdbcUtil.executeUpdateSuccess(tddlConnection, tddlSql);
        String sqlMode =
            "NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION";
        if (isMySQL80()) {
            sqlMode = "NO_ENGINE_SUBSTITUTION";
        }
        try {
            setSqlMode(sqlMode, tddlConnection);
        } catch (Exception e) {
            e.printStackTrace();
        }
        for (String tableName : allLogicalTables) {
            if (tableName.toLowerCase().startsWith("gsi_")) {
                continue;
            }
            String sql = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true)*/delete from " + tableName;
            JdbcUtil.executeUpdate(tddlConnection, sql);
        }
    }

    @Test
    public void testConcurrentWithDML() {
        if (indexSk.equalsIgnoreCase(C_INT_32)) {
            System.out.println(LocalTime.now().toString() + ":" + "start to run the testcase");
        }
        final String sqlMode ;
        if (isMySQL80()) {
            sqlMode = "NO_ENGINE_SUBSTITUTION";
        } else {
            sqlMode =
                "NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION";
        }
        System.out.println(LocalTime.now().toString() + ":" + "start to run the testcase");
        final AtomicBoolean stop = new AtomicBoolean(false);
        String hint =
            "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true,ENABLE_MODIFY_SHARDING_COLUMN=TRUE,PLAN_CACHE=false)*/ ";
        if (isCache == null) {
            hint = "/*+TDDL:CMD_EXTRA(ENABLE_COMPLEX_DML_CROSS_DB=true,ENABLE_MODIFY_SHARDING_COLUMN=TRUE)*/ ";
        } else if (isCache.booleanValue()) {
            hint =
                " /*+TDDL:cmd_extra(ENABLE_COMPLEX_DML_CROSS_DB=true,ENABLE_MODIFY_SHARDING_COLUMN=TRUE,PLAN_CACHE=true)*/ ";
        }
        final String hintStr = hint;
        final List<Future> inserts =
            ImmutableList.of(dmlPool
                    .submit(new DMLRunner(stop, normalTable1, (sql) -> {
                        setSqlMode(sqlMode, tddlConnection);
                        executeDml(hintStr + sql);
                    })),
                dmlPool.submit(new DMLRunner(stop, normalTable2, (sql) -> {
                    setSqlMode(sqlMode, tddlConnection);
                    executeDml(hintStr + sql);
                })));

        try {
            TimeUnit.SECONDS.sleep(10);
        } catch (InterruptedException e) {
            // ignore exception
        }
        stop.set(true);
        for (Future future : inserts) {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
        dmlPool.shutdown();
        System.out.println(LocalTime.now().toString() + ":" + "start to check the result");
        if (finalTableStatus.isWritable() && firstIn) {
            checkDataContentForScaleOutWrite(physicalTablesOnSourceGroup.get(normalTable1.getTableName()), "");
            checkDataContentForScaleOutWrite(physicalTablesOnSourceGroup.get(normalTable2.getTableName()), "");
            //checkDataContentForScaleOutWrite(physicalTablesOnSourceGroup.get("gsi_" + normalTable2.getTableName()), "");
        }
        System.out.println(LocalTime.now().toString() + ":" + "check the result:finish");
    }

    protected void checkSwitchAndCleanCommand() {

        String tddlSql = "show move database";
        try {
            PreparedStatement stmt = JdbcUtil.preparedStatement(tddlSql, tddlConnection);
            ResultSet rs = stmt.executeQuery();
            while (rs.next()) {
                String sourceDbGroupKey = rs.getString("SOURCE_DB_GROUP_KEY");
                String temporaryDbGroupKey = rs.getString("TEMPORARY_DB_GROUP_KEY");
                tddlSql = "move database /*+TDDL:CMD_EXTRA(SWITCH_GROUP_ONLY=true)*/ " + sourceDbGroupKey + " to xx";
                JdbcUtil.executeUpdate(tddlConnection, tddlSql);
                tddlSql = "move database clean " + temporaryDbGroupKey;
                JdbcUtil.executeUpdate(tddlConnection, tddlSql);
            }
            rs.close();
            stmt.close();
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    private class PrepareTable {
        private String indexPk;
        private final String tableDef;
        private List<String> testType;
        private final String tableName;
        private List<String> scaleOutInserts;
        private List<String> scaleOutDeletes;
        private List<String> scaleOutUpdates;

        public PrepareTable(String indexSk, boolean isGsiTable, int tableIndex) {
            if (testType == null) {
                testType = FULL_TYPE_TABLE_COLUMNS.stream().
                    filter(c -> c.toLowerCase().indexOf("int") != -1
                        && c.toLowerCase().indexOf("point") == -1
                        && c.toLowerCase().indexOf("tinyint") == -1
                        && c.toLowerCase().indexOf("smallint") == -1
                        && !c.toLowerCase().endsWith("_1")).collect(Collectors.toList());
            }
            if (tableIndex == 1) {
                tableName = PRIMARY_TABLE_NAME1;
            } else {
                tableName = PRIMARY_TABLE_NAME2;
            }

            this.indexPk = testType.get(random.nextInt(testType.size() - 1));
            while (indexPk.equalsIgnoreCase(indexSk)) {
                this.indexPk = testType.get(random.nextInt(testType.size() - 1));
            }

            final AtomicInteger tmpPkIndex = new AtomicInteger(0);
            final String columnDef = Ord.zip(FULL_TYPE_TABLE_COLUMN_DEFS).stream().map(o -> {
                final String column = o.getValue().left;
                String type = o.getValue().right;

                String autoIncrement = "";
                if (TStringUtil.equalsIgnoreCase(this.indexPk, column)) {
                    tmpPkIndex.set(o.i);
                    autoIncrement = "AUTO_INCREMENT";
                    type = type.replace("DEFAULT NULL", "NOT NULL")
                        .replace("DEFAULT CURRENT_TIMESTAMP", "NOT NULL")
                        .replace("DEFAULT \"2000-01-01 00:00:00\"", "NOT NULL");
                }
                return MessageFormat.format("{0} {1} {2}", column, type, autoIncrement);
            }).collect(Collectors.joining(",\n"));
            final String PKDef = MessageFormat.format(PRIMARY_KEY_TEMPLATE, indexPk);
            final String shardDef = MessageFormat.format(PARTITIONING_DEFINITION, indexSk, indexSk);

            this.tableDef = ExecuteTableSelect
                .getTableDef(tableName, columnDef, PKDef, shardDef);
            scaleOutInserts = ScaleOutConstant
                .getInsertWithShardKey(tableName, indexSk, indexPk);

            scaleOutDeletes = ScaleOutConstant.getDeleteWithShardKey(tableName, indexSk);
            scaleOutUpdates = ScaleOutConstant.getUpdateWithShardKey(tableName, indexSk);
            if (isGsiTable) {
                scaleOutUpdates.addAll(ScaleOutConstant.getUpdateForTwoTable(PRIMARY_TABLE_NAME1, tableName, indexSk));
            }
            System.out
                .println(LocalTime.now().toString() + "Table:" + tableName + " pk: " + indexPk + " sk: " + indexSk);
            // System.out.println(LocalTime.now().toString() + "tableDef: " + tableDef);
        }

        public String getTableDef() {
            return tableDef;
        }

        public String getTableName() {
            return tableName;
        }

        public List<String> getScaleOutDeletes() {
            return scaleOutDeletes;
        }

        public List<String> getScaleOutInserts() {
            return scaleOutInserts;
        }

        public List<String> getScaleOutUpdates() {
            return scaleOutUpdates;
        }
    }

    private final static String PRIMARY_TABLE_NAME1 = "shrd_full_type_tb1";
    private final static String PRIMARY_TABLE_NAME2 = "shrd_full_type_tb2";
    private final String PARTITIONING_DEFINITION =
        " dbpartition by hash({0}) tbpartition by hash({1}) tbpartitions 3";
    private java.util.Random random = new java.util.Random();
    private Set<String> ignoreErr = ImmutableSet.of("Bad format for Time");

    private static class DMLRunner implements Runnable {

        private final AtomicBoolean stop;
        private PrepareTable prepareTable;

        private final Consumer<String> doDmlByScaleOutWrite;

        public DMLRunner(AtomicBoolean stop, PrepareTable preTable, Consumer<String> doDmlByScaleOutWrite) {
            this.stop = stop;
            this.prepareTable = preTable;
            this.doDmlByScaleOutWrite = doDmlByScaleOutWrite;
        }

        @Override
        public void run() {
            try {
                int count = 0;
                do {
                    String insert = prepareTable.getScaleOutInserts()
                        .get(ThreadLocalRandom.current().nextInt(prepareTable.getScaleOutInserts().size()));
                    final String delete = prepareTable.getScaleOutDeletes()
                        .get(ThreadLocalRandom.current().nextInt(prepareTable.getScaleOutDeletes().size()));
                    final String update = prepareTable.getScaleOutUpdates()
                        .get(ThreadLocalRandom.current().nextInt(prepareTable.getScaleOutUpdates().size()));

                    doDmlByScaleOutWrite.accept(insert);
                    doDmlByScaleOutWrite.accept(update);
                    doDmlByScaleOutWrite.accept(delete);

                    insert = prepareTable.getScaleOutInserts()
                        .get(ThreadLocalRandom.current().nextInt(prepareTable.getScaleOutInserts().size()));
                    doDmlByScaleOutWrite.accept(insert);
                    insert = prepareTable.getScaleOutInserts()
                        .get(ThreadLocalRandom.current().nextInt(prepareTable.getScaleOutInserts().size()));
                    doDmlByScaleOutWrite.accept(insert);
                } while (!stop.get());

                System.out.println(Thread.currentThread().getName() + " quit after " + count + " records inserted");
            } catch (Exception e) {
                throw new RuntimeException("Insert failed!", e);
            }

        }

    }

    private List<String> getTableDefs() {
        List<String> createTables = new ArrayList<>();
        createTables.add(normalTable1.getTableDef());
        createTables.add(normalTable2.getTableDef());
        return createTables;
    }
}

class StatueInfoExt extends StatusInfo {
    final String indexSK;

    public StatueInfoExt(ComplexTaskMetaManager.ComplexTaskStatus moveTableStatus, Boolean cache, String indexSk) {
        super(moveTableStatus, cache);
        this.indexSK = indexSk;
    }

    public String getIndexSK() {
        return indexSK;
    }

    public String toString() {
        return moveTableStatus.toString() + "[Plan_Cache=" + (cache != null ? String.valueOf(cache) : "None")
            + ",shardKey=" + indexSK + "]";
    }
}