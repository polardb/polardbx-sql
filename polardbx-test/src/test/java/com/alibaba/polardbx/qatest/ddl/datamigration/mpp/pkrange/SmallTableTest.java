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

@NotThreadSafe
@RunWith(Parameterized.class)
public class SmallTableTest extends DDLBaseNewDBTestCase {

    final static Log log = LogFactory.getLog(PkTest.class);
    private String tableName = "";
    private static final String createOption = " if not exists ";

    public SmallTableTest(boolean crossSchema) {
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
    public void testAddGsiForSmallTable() throws Exception {
        String schemaName = "pk_range_test";
        String originalTableName = "small_table_auto";
        String gsiName = randomTableName("gsi_small_", 8);
        // prepare data
        String createTableStmt =
            "create table if not exists " + " %s(a int NOT NULL AUTO_INCREMENT,b int, c varchar(32), PRIMARY KEY(a)"
                + ") PARTITION BY HASH(a) PARTITIONS %d";
        int partNum = 16;
        int gsiPartNum = 16;
        int eachPartRows = 1024;
        Boolean enablePkRange = true;
        Boolean enableLocalIndexLater = true;
        String addGsiStmt = "alter table %s add global index %s(b) covering(c) partition by hash(b) partitions %d";
        PkRangeTestParam pkRangeTestParam =
            new PkRangeTestParam(schemaName, originalTableName, gsiName, createTableStmt, addGsiStmt, partNum,
                gsiPartNum, eachPartRows, enablePkRange, enableLocalIndexLater);
        PkRangeTestParam.baseTest(pkRangeTestParam, tddlConnection);
    }

}