package com.alibaba.polardbx.qatest.ddl.auto.partition;

import com.clearspring.analytics.util.Lists;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.SQLException;
import java.util.List;

/**
 * @author chenghui.lch
 */
public class SubPartitionTableRndDdlTest extends PartTableRndDdlTestBase {

    public SubPartitionTableRndDdlTest(SubPartitionTableRndDdlTest.TestParameter parameter) {
        super(parameter);
    }

    @Parameterized.Parameters(name = "{index}: rndDdlTestCase {0}")
    public static List<SubPartitionTableRndDdlTest.TestParameter> parameters() {
        //initDdlTestEnv();
        //return PartTableRndDdlTestBase.buildTestParams();
        return Lists.newArrayList();
    }

    @Ignore
    public void runTest() throws SQLException {
        testRandomDdl();
    }
}
