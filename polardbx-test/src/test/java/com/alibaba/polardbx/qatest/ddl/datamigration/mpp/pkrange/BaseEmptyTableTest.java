package com.alibaba.polardbx.qatest.ddl.datamigration.mpp.pkrange;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.ddl.datamigration.mpp.base.PkRangeTestParam;
import net.jcip.annotations.NotThreadSafe;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.twoPhaseDdl.TwoPhaseDdlTestUtils.DataManipulateUtil.prepareData;

@NotThreadSafe
@RunWith(Parameterized.class)
public class BaseEmptyTableTest extends DDLBaseNewDBTestCase {

    final static Log log = LogFactory.getLog(PkTest.class);
    private String tableName = "";
    private static final String createOption = " if not exists ";

    public BaseEmptyTableTest(boolean crossSchema) {
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
    public void testLegacyAddGsiForEmptyTable() throws Exception {
        String schemaName = "pk_range_test";
        String originalTableName = "empty_table_for_simple";
        String gsiName = randomTableName("gsi_base_empty_", 8);
        String createTableStmt =
            "create table if not exists " + " %s(a int NOT NULL AUTO_INCREMENT,b int, c varchar(32), PRIMARY KEY(a)"
                + ") PARTITION BY HASH(a) PARTITIONS %d";
        int partNum = 16;
        int gsiPartNum = 16;
        int eachPartRows = 0;
        Boolean enablePkRange = false;
        Boolean enableLocalIndexLater = false;
        String addGsiStmt = "alter table %s add global index %s(b) covering(c) partition by hash(b) partitions %d";
        PkRangeTestParam pkRangeTestParam =
            new PkRangeTestParam(schemaName, originalTableName, gsiName, createTableStmt, addGsiStmt, partNum,
                gsiPartNum, eachPartRows, enablePkRange, enableLocalIndexLater);
        PkRangeTestParam.baseTest(pkRangeTestParam, tddlConnection);
    }

    @Test
    public void testLegacyAddUGsiForEmptyTable() throws Exception {
        String schemaName = "pk_range_test";
        String originalTableName = "empty_table_for_simple_ugsi";
        String gsiName = randomTableName("gsi_base_unique_empty_", 8);
        String createTableStmt =
            "create table if not exists " + " %s(a int NOT NULL AUTO_INCREMENT,b int, c varchar(32), PRIMARY KEY(a)"
                + ") PARTITION BY HASH(a) PARTITIONS %d";
        int partNum = 16;
        int gsiPartNum = 16;
        int eachPartRows = 0;
        Boolean enablePkRange = false;
        Boolean enableLocalIndexLater = false;
        String addGsiStmt =
            "alter table %s add global unique index %s(b) covering(c) partition by hash(b) partitions %d";
        PkRangeTestParam pkRangeTestParam =
            new PkRangeTestParam(schemaName, originalTableName, gsiName, createTableStmt, addGsiStmt, partNum,
                gsiPartNum, eachPartRows, enablePkRange, enableLocalIndexLater);
        PkRangeTestParam.baseTest(pkRangeTestParam, tddlConnection);
    }

    @Test
    public void testLegacyAddUGsiForceDenyForEmptyTable() throws Exception {
        String schemaName = "pk_range_test";
        String originalTableName = "empty_table_for_simple_ugsi_force_deny";
        String gsiName = randomTableName("gsi_base_unique2_empty_", 8);
        String createTableStmt =
            "create table if not exists " + " %s(a int NOT NULL AUTO_INCREMENT,b int, c varchar(32), PRIMARY KEY(a)"
                + ") PARTITION BY HASH(a) PARTITIONS %d";
        int partNum = 16;
        int gsiPartNum = 16;
        int eachPartRows = 0;
        Boolean enablePkRange = true;
        Boolean enableLocalIndexLater = true;
        String addGsiStmt =
            "alter table %s add global unique index %s(b) covering(c) partition by hash(b) partitions %d";
        PkRangeTestParam pkRangeTestParam =
            new PkRangeTestParam(schemaName, originalTableName, gsiName, createTableStmt, addGsiStmt, partNum,
                gsiPartNum, eachPartRows, enablePkRange, enableLocalIndexLater);
        pkRangeTestParam.setExpectedPkRangeNum(0);
        pkRangeTestParam.setExpectedPkRange(false);
        pkRangeTestParam.setExpectedLocalIndexLater(false);
        PkRangeTestParam.baseTest(pkRangeTestParam, tddlConnection);
    }

    @Test
    public void testLegacyCreateEmptyTableWithGsi() throws Exception {
        String schemaName = "pk_range_test";
        String originalTableName = "empty_table_with_gsi";
        String gsiName = randomTableName("gsi_empty1_", 8);
        String createTableWithGsiStmt =
            "create table %s(a int NOT NULL AUTO_INCREMENT,b int, c varchar(32), PRIMARY KEY(a)"
                + ", global index %s(b) partition by hash(b) partitions 16 "
                + ") PARTITION BY HASH(a) PARTITIONS %d";
        int partNum = 16;
        int gsiPartNum = 16;
        int eachPartRows = 0;
        Boolean enablePkRange = false;
        Boolean enableLocalIndexLater = false;
        PkRangeTestParam pkRangeTestParam =
            new PkRangeTestParam(schemaName, originalTableName, gsiName, "", createTableWithGsiStmt, partNum,
                gsiPartNum, eachPartRows, enablePkRange, enableLocalIndexLater);
        PkRangeTestParam.baseTest(pkRangeTestParam, tddlConnection);
    }

    @Test
    public void testLegacyCreateEmptyTableWithGsiForceDeny() throws Exception {
        String schemaName = "pk_range_test";
        String originalTableName = "empty_table_with_gsi_force_deny";
        String gsiName = randomTableName("gsi_empty2_", 8);
        String createTableWithGsiStmt =
            "create table %s(a int NOT NULL AUTO_INCREMENT,b int, c varchar(32), PRIMARY KEY(a)"
                + ", global index %s(b) partition by hash(b) partitions 16 "
                + ") PARTITION BY HASH(a) PARTITIONS %d";
        int partNum = 16;
        int gsiPartNum = 16;
        int eachPartRows = 0;
        Boolean enablePkRange = true;
        Boolean enableLocalIndexLater = true;
        PkRangeTestParam pkRangeTestParam =
            new PkRangeTestParam(schemaName, originalTableName, gsiName, "", createTableWithGsiStmt, partNum,
                gsiPartNum, eachPartRows, enablePkRange, enableLocalIndexLater);
        pkRangeTestParam.setExpectedPkRangeNum(0);
        pkRangeTestParam.setExpectedPkRange(false);
        pkRangeTestParam.setExpectedLocalIndexLater(false);
        PkRangeTestParam.baseTest(pkRangeTestParam, tddlConnection);
    }

    @Test
    public void testAddGsiForEmptyTable() throws Exception {
        String schemaName = "pk_range_test";
        String originalTableName = "empty_table_empty_auto";
        String gsiName = randomTableName("gsi_empty_", 8);
        // prepare data
        String createTableStmt =
            "create table if not exists " + " %s(a int NOT NULL AUTO_INCREMENT,b int, c varchar(32), PRIMARY KEY(a)"
                + ") PARTITION BY HASH(a) PARTITIONS %d";
        int partNum = 16;
        int gsiPartNum = 16;
        int eachPartRows = 0;
        Boolean enablePkRange = true;
        Boolean enableLocalIndexLater = true;
        String addGsiStmt = "alter table %s add global index %s(b) covering(c) partition by hash(b) partitions %d";
        PkRangeTestParam pkRangeTestParam =
            new PkRangeTestParam(schemaName, originalTableName, gsiName, createTableStmt, addGsiStmt, partNum,
                gsiPartNum, eachPartRows, enablePkRange, enableLocalIndexLater);
        PkRangeTestParam.baseTest(pkRangeTestParam, tddlConnection);
    }

    @Test
    public void testAddGsiForEmptyTableOnLocalIndex1() throws Exception {
        String schemaName = "pk_range_test";
        String originalTableName = "empty_table_local_index1";
        String gsiName = randomTableName("gsi_empty_", 8);
        // prepare data
        String createTableStmt = "create table if not exists "
            + " %s(a int NOT NULL AUTO_INCREMENT,b int, c varchar(32), d TIMESTAMP, PRIMARY KEY(a),"
            + " local index i_c(c), local index i_bc(b, c), local index i_d(d)) PARTITION BY HASH(a) PARTITIONS %d";
        int partNum = 16;
        int gsiPartNum = 16;
        int eachPartRows = 0;
        Boolean enablePkRange = true;
        Boolean enableLocalIndexLater = true;
        String addGsiStmt = "alter table %s add global index %s(b) covering(c) partition by hash(b) partitions %d";
        PkRangeTestParam pkRangeTestParam =
            new PkRangeTestParam(schemaName, originalTableName, gsiName, createTableStmt, addGsiStmt, partNum,
                gsiPartNum, eachPartRows, enablePkRange, enableLocalIndexLater);
        PkRangeTestParam.baseTest(pkRangeTestParam, tddlConnection);
    }

    @Test
    public void testAddGsiForEmptyTableOnLocalIndex2() throws Exception {
        String schemaName = "pk_range_test";
        String originalTableName = "empty_table_local_index2";
        String gsiName = randomTableName("gsi_empty_", 8);
        // prepare data
        String createTableStmt = "create table if not exists "
            + " %s(a int NOT NULL AUTO_INCREMENT,b int, c varchar(32), d TIMESTAMP, PRIMARY KEY(a),"
            + " local index i_c(c), local unique index i_bc(b, c), local index i_d(d)) PARTITION BY HASH(a) PARTITIONS %d";
        int partNum = 16;
        int gsiPartNum = 16;
        int eachPartRows = 0;
        Boolean enablePkRange = true;
        Boolean enableLocalIndexLater = true;
        String addGsiStmt = "alter table %s add global index %s(b, c, d) partition by hash(b) partitions %d";
        PkRangeTestParam pkRangeTestParam =
            new PkRangeTestParam(schemaName, originalTableName, gsiName, createTableStmt, addGsiStmt, partNum,
                gsiPartNum, eachPartRows, enablePkRange, enableLocalIndexLater);
        PkRangeTestParam.baseTest(pkRangeTestParam, tddlConnection);
    }

    @Test
    public void testAddClusteredGsiForEmptyTableOnLocalIndex1() throws Exception {
        String schemaName = "pk_range_test";
        String originalTableName = "empty_table_csi_local_index1";
        String gsiName = randomTableName("csi_empty_", 8);
        // prepare data
        String createTableStmt = "create table if not exists "
            + " %s(a int NOT NULL AUTO_INCREMENT,b int, c varchar(32), d TIMESTAMP, PRIMARY KEY(a),"
            + " local index i_c(c), local index i_bc(b, c), local index i_d(d)) PARTITION BY HASH(a) PARTITIONS %d";
        int partNum = 16;
        int gsiPartNum = 16;
        int eachPartRows = 0;
        Boolean enablePkRange = true;
        Boolean enableLocalIndexLater = true;
        String addGsiStmt = "alter table %s add clustered index %s(b) partition by hash(b) partitions %d";
        PkRangeTestParam pkRangeTestParam =
            new PkRangeTestParam(schemaName, originalTableName, gsiName, createTableStmt, addGsiStmt, partNum,
                gsiPartNum, eachPartRows, enablePkRange, enableLocalIndexLater);
        PkRangeTestParam.baseTest(pkRangeTestParam, tddlConnection);
    }

    @Test
    public void testAddClusteredGsiForEmptyTableOnLocalIndex2() throws Exception {
        String schemaName = "pk_range_test";
        String originalTableName = "empty_table_csi_local_index2";
        String gsiName = randomTableName("csi_empty_", 8);
        // prepare data
        String createTableStmt = "create table if not exists "
            + " %s(a int NOT NULL AUTO_INCREMENT,b int, c varchar(32), d TIMESTAMP, PRIMARY KEY(a),"
            + " local index i_c(c), local unique index i_bc(b, c), local index i_d(d)) PARTITION BY HASH(a) PARTITIONS %d";
        int partNum = 16;
        int gsiPartNum = 16;
        int eachPartRows = 0;
        Boolean enablePkRange = true;
        Boolean enableLocalIndexLater = true;
        String addGsiStmt = "alter table %s add clustered index %s(b) partition by hash(b) partitions %d";
        PkRangeTestParam pkRangeTestParam =
            new PkRangeTestParam(schemaName, originalTableName, gsiName, createTableStmt, addGsiStmt, partNum,
                gsiPartNum, eachPartRows, enablePkRange, enableLocalIndexLater);
        PkRangeTestParam.baseTest(pkRangeTestParam, tddlConnection);
    }

}