package com.alibaba.polardbx.qatest.ddl.auto.partition;

import org.junit.runners.Parameterized;

import java.util.List;

/**
 * @author chenghui.lch
 */
public class PartitionIntraGroupParallel2Test extends PartitionAutoLoadSqlTestBase {

    public PartitionIntraGroupParallel2Test(AutoLoadSqlTestCaseParams parameter) {
        super(parameter);
    }

    @Parameterized.Parameters(name = "{index}: SubTestCase {0}")
    public static List<AutoLoadSqlTestCaseParams> parameters() {
        return getParameters(PartitionIntraGroupParallel2Test.class, 0, false);
    }
}
