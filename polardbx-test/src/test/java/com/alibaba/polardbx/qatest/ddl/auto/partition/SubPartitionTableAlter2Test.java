package com.alibaba.polardbx.qatest.ddl.auto.partition;

import org.junit.runners.Parameterized;

import java.util.List;

/**
 * @author chenghui.lch
 */
public class SubPartitionTableAlter2Test extends PartitionAutoLoadSqlTestBase {

    public SubPartitionTableAlter2Test(AutoLoadSqlTestCaseParams parameter) {
        super(parameter);
    }

    @Parameterized.Parameters(name = "{index}: SubTestCase {0}")
    public static List<AutoLoadSqlTestCaseParams> parameters() {
        return getParameters(SubPartitionTableAlter2Test.class, 0, false);
    }
}
