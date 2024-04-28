package com.alibaba.polardbx.qatest.ddl.auto.partition;

import org.junit.runners.Parameterized;

import java.util.List;

/**
 * @author chenghui.lch
 */

public class PartitionTable2Test extends PartitionAutoLoadSqlTestBase {

    public PartitionTable2Test(AutoLoadSqlTestCaseParams parameter) {
        super(parameter);
    }

    @Parameterized.Parameters(name = "{index}: SubTestCase {0}")
    public static List<AutoLoadSqlTestCaseParams> parameters() {

        return getParameters(PartitionTable2Test.class, 0, false);
    }

}
