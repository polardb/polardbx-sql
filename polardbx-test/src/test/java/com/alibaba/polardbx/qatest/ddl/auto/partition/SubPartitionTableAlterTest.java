package com.alibaba.polardbx.qatest.ddl.auto.partition;

import org.junit.runners.Parameterized;

import java.util.List;

/**
 * @author chenghui.lch
 */
public class SubPartitionTableAlterTest extends PartitionAutoLoadSqlTestBase {

    public SubPartitionTableAlterTest(AutoLoadSqlTestCaseParams parameter) {
        super(parameter);
    }

    @Parameterized.Parameters(name = "{index}: SubTestCase {0}")
    public static List<AutoLoadSqlTestCaseParams> parameters() {
        return getParameters(SubPartitionTableAlterTest.class, 0, false);
    }
}
