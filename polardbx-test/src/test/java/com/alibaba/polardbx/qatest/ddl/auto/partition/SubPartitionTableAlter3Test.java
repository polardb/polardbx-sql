package com.alibaba.polardbx.qatest.ddl.auto.partition;

import org.junit.runners.Parameterized;

import java.util.List;

/**
 * @author chenghui.lch
 */
public class SubPartitionTableAlter3Test extends PartitionAutoLoadSqlTestBase {

    public SubPartitionTableAlter3Test(AutoLoadSqlTestCaseParams parameter) {
        super(parameter);
    }

    @Parameterized.Parameters(name = "{index}: SubTestCase {0}")
    public static List<AutoLoadSqlTestCaseParams> parameters() {
        return getParameters(SubPartitionTableAlter3Test.class, 0, false);
    }
}
