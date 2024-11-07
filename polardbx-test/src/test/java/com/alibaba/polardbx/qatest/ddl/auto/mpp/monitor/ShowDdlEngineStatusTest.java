package com.alibaba.polardbx.qatest.ddl.auto.mpp.monitor;

import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.ddl.auto.mpp.base.PkRangeTestParam;
import net.jcip.annotations.NotThreadSafe;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import static com.alibaba.polardbx.qatest.ddl.auto.mpp.base.PkRangeTestParam.showDdlTest;

@NotThreadSafe
@RunWith(Parameterized.class)
public class ShowDdlEngineStatusTest extends DDLBaseNewDBTestCase {

    final static Log log = LogFactory.getLog(ShowDdlEngineStatusTest.class);
    private String tableName = "";
    private static final String createOption = " if not exists ";

    public ShowDdlEngineStatusTest(boolean crossSchema) {
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
    public void testShowDdlEngineForGsiBackfill() throws Exception {
        String schemaName = "show_ddl_engine_test";
        String originalTableName = "multiple_pk_table1";
        String gsiName = randomTableName("gsi_multiple_pk1_", 8);
        // prepare data
        String createTableStmt = "create table if not exists "
            + " %s(d int, a int NOT NULL AUTO_INCREMENT,b char(16), c varchar(32), PRIMARY KEY(c, a, b)"
            + ") PARTITION BY HASH(a) PARTITIONS %d";
        int partNum = 16;
        int eachPartRows = 102400;
        Boolean enablePkRange = true;
        Boolean enableLocalIndexLater = true;
        int gsiPartNum = 21;
        String addGsiStmt = "alter table %s add global index %s(b) covering(c, a) partition by key(b, c) partitions %d";
        PkRangeTestParam
            pkRangeTestParam =
            new PkRangeTestParam(schemaName, originalTableName, gsiName, createTableStmt, addGsiStmt, partNum,
                gsiPartNum, eachPartRows, enablePkRange, enableLocalIndexLater);
        showDdlTest(pkRangeTestParam, tddlConnection, true, true, false);
    }

    @Test
    public void testShowDdlEngineForMovePartition() throws Exception {
        String schemaName = "show_ddl_engine_test";
        String originalTableName = "multiple_pk_table2";
        String gsiName = randomTableName("gsi_multiple_pk2_", 8);
        // prepare data
        String createTableStmt = "create table if not exists "
            + " %s(d int, a int NOT NULL AUTO_INCREMENT,b char(16), c varchar(32), PRIMARY KEY(c, a, b)"
            + ") PARTITION BY HASH(a) PARTITIONS %d";
        int partNum = 4;
        int eachPartRows = 40960;
        Boolean enablePkRange = true;
        Boolean enableLocalIndexLater = true;
        int gsiPartNum = 21;
//        String addGsiStmt = "";
        String addGsiStmt = "rebalance table %s shuffle_data_dist=1";
        PkRangeTestParam
            pkRangeTestParam =
            new PkRangeTestParam(schemaName, originalTableName, gsiName, createTableStmt, addGsiStmt, partNum,
                gsiPartNum, eachPartRows, enablePkRange, enableLocalIndexLater);
        try {
            showDdlTest(pkRangeTestParam, tddlConnection, true, true, true);
        }catch (Exception e){
            logger.info(StringUtils.isEmpty(e.getMessage())?"bad exception catched":e.getMessage());
            throw e;
        }
    }
}