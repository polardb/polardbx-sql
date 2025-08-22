package com.alibaba.polardbx.qatest.ddl.datamigration.mpp.monitor;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.ddl.datamigration.mpp.base.PkRangeTestParam;
import com.alibaba.polardbx.qatest.twoPhaseDdl.TwoPhaseDdlTestUtils.DdlStateCheckUtil;
import net.jcip.annotations.NotThreadSafe;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.ddl.datamigration.mpp.base.PkRangeTestParam.showDdlTest;

@NotThreadSafe
@RunWith(Parameterized.class)
public class ShowDdlTest extends DDLBaseNewDBTestCase {

    final static Log log = LogFactory.getLog(ShowDdlTest.class);
    private String tableName = "";
    private static final String createOption = " if not exists ";

    public ShowDdlTest(boolean crossSchema) {
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
    public void testAddGsiForLargeTableGsi() throws Exception {
        String schemaName = "show_ddl_test";
        String originalTableName = "multiple_pk_table1";
        String gsiName = randomTableName("gsi_multiple_pk1_", 8);
        // prepare data
        String createTableStmt = "create table if not exists "
            + " %s(d int, a int NOT NULL AUTO_INCREMENT,b char(16), c varchar(32), PRIMARY KEY(c, a, b)"
            + ") PARTITION BY HASH(a) PARTITIONS %d";
        int partNum = 16;
        int eachPartRows = 4096;
        Boolean enablePkRange = true;
        Boolean enableLocalIndexLater = true;
        String addGsiStmt = "alter table %s add global index %s(b) covering(c, a) partition by hash(b) partitions %d";
        PkRangeTestParam
            pkRangeTestParam =
            new PkRangeTestParam(schemaName, originalTableName, gsiName, createTableStmt, addGsiStmt, partNum, partNum,
                eachPartRows, enablePkRange, enableLocalIndexLater);
        showDdlTest(pkRangeTestParam, tddlConnection, false);
    }

    @Test
    public void testAddGsiForLargeTableGsiWithSubjob() throws Exception {
        String schemaName = "show_ddl_test";
        String originalTableName = "multiple_pk_table2";
        String gsiName = randomTableName("gsi_multiple_pk2_", 8);
        // prepare data
        String createTableStmt = "create table if not exists "
            + " %s(d int, a int NOT NULL AUTO_INCREMENT,b char(16), c varchar(32), PRIMARY KEY(c, a, b)"
            + ") PARTITION BY HASH(a) PARTITIONS %d";
        int partNum = 64;
        int eachPartRows = 10240;
        Boolean enablePkRange = true;
        Boolean enableLocalIndexLater = true;
        int gsiPartNum = 21;
        String addGsiStmt = "alter table %s add global index %s(b) covering(c, a) partition by key(b, c) partitions %d";
        PkRangeTestParam
            pkRangeTestParam =
            new PkRangeTestParam(schemaName, originalTableName, gsiName, createTableStmt, addGsiStmt, partNum,
                gsiPartNum, eachPartRows, enablePkRange, enableLocalIndexLater);
        showDdlTest(pkRangeTestParam, tddlConnection, true);
        DdlStateCheckUtil.dropDbAndCheckArchived(schemaName, tddlConnection);
    }

}