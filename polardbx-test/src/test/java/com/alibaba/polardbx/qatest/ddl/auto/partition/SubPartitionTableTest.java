package com.alibaba.polardbx.qatest.ddl.auto.partition;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;

/**
 * @author chenghui.lch
 */
@RunWith(value = Parameterized.class)
public class SubPartitionTableTest extends PartitionAutoLoadSqlTestBase {

    public SubPartitionTableTest(AutoLoadSqlTestCaseParams parameter) {
        super(parameter);
    }

    @Parameterized.Parameters(name = "{index}: SubTestCase {0}")
    public static List<AutoLoadSqlTestCaseParams> parameters() {
        return getParameters(SubPartitionTableTest.class, 0, false);
    }

}
