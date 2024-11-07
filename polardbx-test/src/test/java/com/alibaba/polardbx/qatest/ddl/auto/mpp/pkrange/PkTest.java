package com.alibaba.polardbx.qatest.ddl.auto.mpp.pkrange;

import com.alibaba.polardbx.qatest.BinlogIgnore;
import com.alibaba.polardbx.qatest.CdcIgnore;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.ddl.auto.mpp.base.PkRangeTestParam;
import com.alibaba.polardbx.qatest.ddl.balancer.datagenerator.DataLoader;
import com.alibaba.polardbx.qatest.twoPhaseDdl.TwoPhaseDdlTestUtils.DataManipulateUtil;
import com.alibaba.polardbx.qatest.twoPhaseDdl.TwoPhaseDdlTestUtils.DdlStateCheckUtil;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.Lists;
import net.jcip.annotations.NotThreadSafe;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.qatest.ddl.auto.mpp.base.PkRangeTestParam.baseTest;
import static com.alibaba.polardbx.qatest.ddl.auto.mpp.base.PrivateDataGenerator.generateDuplicatePkData;
import static com.alibaba.polardbx.qatest.twoPhaseDdl.TwoPhaseDdlTestUtils.DataManipulateUtil.prepareData;

@NotThreadSafe
public class PkTest extends DDLBaseNewDBTestCase {

    final static Log log = LogFactory.getLog(PkTest.class);
    private String tableName = "";
    private static final String createOption = " if not exists ";

    public PkTest(boolean crossSchema) {
        this.crossSchema = crossSchema;
    }

    @Parameterized.Parameters(name = "{index}:crossSchema={0}")
    public static List<Object[]> initParameters() {
        return Arrays.asList(new Object[][] {
            {false}});
    }

    @Before
    public void init() {
        this.tableName = schemaPrefix + randomTableName("empty_table", 4);
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    @Test
    public void testAddGsiForMultiplePkTable1() throws Exception {
        String schemaName = "pk_range_test";
        String originalTableName = "multiple_pk_table1";
        String gsiName = randomTableName("gsi_multiple_pk1_", 8);
        // prepare data
        String createTableStmt = "create table if not exists "
            + " %s(d int, a int NOT NULL AUTO_INCREMENT,b char(16), c varchar(32), PRIMARY KEY(c, a, b)"
            + ") PARTITION BY HASH(a) PARTITIONS %d";
        int partNum = 16;
        int eachPartRows = 1024;
        Boolean enablePkRange = true;
        Boolean enableLocalIndexLater = true;
        String addGsiStmt = "alter table %s add global index %s(b) covering(c, a) partition by hash(b) partitions %d";
        PkRangeTestParam pkRangeTestParam =
            new PkRangeTestParam(schemaName, originalTableName, gsiName, createTableStmt, addGsiStmt, partNum, partNum,
                eachPartRows, enablePkRange, enableLocalIndexLater);
        baseTest(pkRangeTestParam, tddlConnection);
    }

    @Test
    public void testAddGsiForMultiplePkTable2() throws Exception {
        String schemaName = "pk_range_test";
        String originalTableName = "multiple_pk_table2";
        String gsiName = randomTableName("gsi_multiple_pk2_", 8);
        // prepare data
        String createTableStmt = "create table if not exists "
            + " %s(d int, a int NOT NULL AUTO_INCREMENT,b DATETIME not null default current_timestamp, c varchar(32), PRIMARY KEY(c, a, b)"
            + ") PARTITION BY HASH(a) PARTITIONS %d";
        int partNum = 16;
        int eachPartRows = 1024;
        Boolean enablePkRange = true;
        Boolean enableLocalIndexLater = true;
        String addGsiStmt = "alter table %s add global index %s(b) covering(c, a) partition by hash(b) partitions %d";
        PkRangeTestParam pkRangeTestParam =
            new PkRangeTestParam(schemaName, originalTableName, gsiName, createTableStmt, addGsiStmt, partNum, partNum,
                eachPartRows, enablePkRange, enableLocalIndexLater);
        baseTest(pkRangeTestParam, tddlConnection);
    }

    @Test
    @CdcIgnore(ignoreReason = "ignore handly insert duplicate key")
    public void testAddGsiOnDuplicateKey() throws Exception {
        String schemaName = "pk_range_test";
        String originalTableName = "multiple_pk_table13";
        String gsiName = randomTableName("gsi_multiple_pk13_", 8);
        // prepare data
        String createTableStmt = "create table if not exists "
            + " %s(a int, b varchar(16), c varchar(32), d int, e int, PRIMARY KEY(a)"
            + ") PARTITION BY HASH(b) PARTITIONS %d";
        int partNum = 16;
        int gsiPartNum = partNum;
        int eachPartRows = 1024;
        Boolean enablePkRange = true;
        Boolean enableLocalIndexLater = true;
        String addGsiStmt = "alter table %s add global index %s(c) covering(b, a) partition by hash(c) partitions %d";
        PkRangeTestParam pkRangeTestParam =
            new PkRangeTestParam(schemaName, originalTableName, gsiName, createTableStmt, addGsiStmt, partNum, partNum,
                eachPartRows, enablePkRange, enableLocalIndexLater);
        Boolean buildLocalIndexLater = pkRangeTestParam.enableLocalIndexLater;
        String taskRangeSize = pkRangeTestParam.taskRangeSize;
        String pkRangeSize = pkRangeTestParam.pkRangeSize;
        long expectedPkRangeNum = pkRangeTestParam.expectedPkRangeNum;
        int expectedLocalIndexConcurrency = pkRangeTestParam.expectedLocalIndexConcurrency;
        Boolean expectedBuildLocalIndexLater = pkRangeTestParam.expectedLocalIndexLater;
        Boolean expectedPkRange = pkRangeTestParam.expectedPkRange;
        String addGsiDdl = String.format(addGsiStmt, originalTableName, gsiName, gsiPartNum);
        Connection connection = tddlConnection;
        String addGsiHint =
            String.format(
                "/*+TDDL:CMD_EXTRA(GSI_BACKFILL_BY_PK_RANGE=%s,GSI_BUILD_LOCAL_INDEX_LATER=%s,BACKFILL_MAX_TASK_PK_RANGE_SIZE=%s,BACKFILL_MAX_PK_RANGE_SIZE=%s,"
                    + "GSI_JOB_MAX_PARALLELISM=16,GSI_PK_RANGE_CPU_ACQUIRE=6)*/",
                enablePkRange, buildLocalIndexLater, taskRangeSize, pkRangeSize);
        prepareData(connection, schemaName, originalTableName, eachPartRows, createTableStmt,
            partNum, DataManipulateUtil.TABLE_TYPE.SELF_DEF_TABLE);

        List<String> columnNames = Lists.newArrayList("a", "b", "c", "d", "e");
        List<String> columnTypes =
            Arrays.stream("int,varchar(16),varchar(32),int,int".split(",")).collect(Collectors.toList());
        int pkColumnIndex = 0;
        int partitionColumnIndex = 1;
        List<Map<String, Object>> rows =
            generateDuplicatePkData(tddlConnection, schemaName, originalTableName, columnNames, columnTypes,
                pkColumnIndex, partitionColumnIndex, partNum,
                eachPartRows, 0);

        String truncateTableSql = String.format("truncate table %s", originalTableName);
        DdlStateCheckUtil.alterTableViaJdbc(connection, schemaName, originalTableName, truncateTableSql);
        DataLoader dataLoader =
            DataLoader.create(tddlConnection, originalTableName, rows);
        dataLoader.batchInsertFromRow(rows.size(), true);

        String analyzeTableSql = String.format("analyze table %s", originalTableName);
        DdlStateCheckUtil.alterTableViaJdbc(connection, schemaName, originalTableName, analyzeTableSql);
        JdbcUtil.executeFailed(connection, addGsiHint + addGsiDdl, "Duplicated entry ");
    }

    @Test
    public void testAddGsiForMultiplePkTable4() throws Exception {
        String schemaName = "pk_range_test";
        String originalTableName = "multiple_pk_table4";
        String gsiName = randomTableName("gsi_multiple_pk4_", 8);
        // prepare data
        String createTableStmt = "create table if not exists "
            + " %s(d int, a int NOT NULL AUTO_INCREMENT,b TIMESTAMP not null, c varchar(32), PRIMARY KEY(c, a, b)"
            + ") PARTITION BY HASH(a) PARTITIONS %d";
        int partNum = 8;
        int eachPartRows = 1024;
        Boolean enablePkRange = true;
        Boolean enableLocalIndexLater = true;
        String addGsiStmt = "alter table %s add global index %s(b) covering(c, a) partition by hash(b) partitions %d";
        PkRangeTestParam pkRangeTestParam =
            new PkRangeTestParam(schemaName, originalTableName, gsiName, createTableStmt, addGsiStmt, partNum, partNum,
                eachPartRows, enablePkRange, enableLocalIndexLater);
        baseTest(pkRangeTestParam, tddlConnection);
    }

    @Test
    public void testAddGsiForMultiplePkTable5() throws Exception {
        String schemaName = "pk_range_test";
        String originalTableName = "multiple_pk_table5";
        String gsiName = randomTableName("gsi_multiple_pk5_", 8);
        // prepare data
        String createTableStmt = "create table if not exists "
            + " %s(d int, a int NOT NULL AUTO_INCREMENT,b DATETIME not null default current_timestamp, c decimal(10, 3), PRIMARY KEY(c, a, b)"
            + ") PARTITION BY HASH(a) PARTITIONS %d";
        int partNum = 8;
        int eachPartRows = 1024;
        Boolean enablePkRange = true;
        Boolean enableLocalIndexLater = true;
        String addGsiStmt = "alter table %s add global index %s(b) covering(c, a) partition by hash(b) partitions %d";
        PkRangeTestParam pkRangeTestParam =
            new PkRangeTestParam(schemaName, originalTableName, gsiName, createTableStmt, addGsiStmt, partNum, partNum,
                eachPartRows, enablePkRange, enableLocalIndexLater);
        baseTest(pkRangeTestParam, tddlConnection);
    }

    @Test
    public void testAddGsiForMultiplePkTable6() throws Exception {
        String schemaName = "pk_range_test";
        String originalTableName = "multiple_pk_table6";
        String gsiName = randomTableName("gsi_multiple_pk6_", 8);
        // prepare data
        String createTableStmt = "create table if not exists "
            + " %s(d int, a int NOT NULL AUTO_INCREMENT,b DATE not null, c time, PRIMARY KEY(c, a, b)"
            + ") PARTITION BY HASH(a) PARTITIONS %d";
        int partNum = 8;
        int eachPartRows = 1024;
        Boolean enablePkRange = true;
        Boolean enableLocalIndexLater = true;
        String addGsiStmt = "alter table %s add global index %s(b) covering(c, a) partition by hash(b) partitions %d";
        PkRangeTestParam pkRangeTestParam =
            new PkRangeTestParam(schemaName, originalTableName, gsiName, createTableStmt, addGsiStmt, partNum, partNum,
                eachPartRows, enablePkRange, enableLocalIndexLater);
        baseTest(pkRangeTestParam, tddlConnection);
    }

    @Test
    public void testAddGsiForMultiplePkTable7() throws Exception {
        String schemaName = "pk_range_test";
        String originalTableName = "multiple_pk_table7";
        String gsiName = randomTableName("gsi_multiple_pk7_", 8);
        // prepare data
        String createTableStmt = "create table if not exists "
            + " %s(d int, a int NOT NULL AUTO_INCREMENT,b datetime, c year, PRIMARY KEY(c, a, b)"
            + ") PARTITION BY HASH(a) PARTITIONS %d";
        int partNum = 8;
        int eachPartRows = 1024;
        Boolean enablePkRange = true;
        Boolean enableLocalIndexLater = true;
        String addGsiStmt = "alter table %s add global index %s(b) covering(c, a) partition by hash(b) partitions %d";
        PkRangeTestParam pkRangeTestParam =
            new PkRangeTestParam(schemaName, originalTableName, gsiName, createTableStmt, addGsiStmt, partNum, partNum,
                eachPartRows, enablePkRange, enableLocalIndexLater);
        baseTest(pkRangeTestParam, tddlConnection);
    }

    @Test
    public void testAddGsiForMultiplePkTable8() throws Exception {
        String schemaName = "pk_range_test";
        String originalTableName = "multiple_pk_table8";
        String gsiName = randomTableName("gsi_multiple_pk8_", 8);
        // prepare data
        String createTableStmt = "create table if not exists "
            + " %s(d int, a int NOT NULL AUTO_INCREMENT,b binary(20), c timestamp, PRIMARY KEY(c, a, b)"
            + ") PARTITION BY HASH(a) PARTITIONS %d";
        int partNum = 8;
        int eachPartRows = 1024;
        Boolean enablePkRange = true;
        Boolean enableLocalIndexLater = true;
        String addGsiStmt = "alter table %s add global index %s(a) covering(c, b) partition by hash(a) partitions %d";
        PkRangeTestParam pkRangeTestParam =
            new PkRangeTestParam(schemaName, originalTableName, gsiName, createTableStmt, addGsiStmt, partNum, partNum,
                eachPartRows, enablePkRange, enableLocalIndexLater);
        baseTest(pkRangeTestParam, tddlConnection);
    }

}