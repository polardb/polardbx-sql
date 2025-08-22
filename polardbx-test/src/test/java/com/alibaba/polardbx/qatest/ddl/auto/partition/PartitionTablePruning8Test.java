package com.alibaba.polardbx.qatest.ddl.auto.partition;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;

/**
 * @author chenghui.lch
 */
@RunWith(value = Parameterized.class)
public class PartitionTablePruning8Test extends PartitionAutoLoadSqlTestBase {

    public PartitionTablePruning8Test(AutoLoadSqlTestCaseParams parameter) {
        super(parameter);
    }

    @Parameterized.Parameters(name = "{index}: SubTestCase {0}")
    public static List<AutoLoadSqlTestCaseParams> parameters() {
        return getParameters(PartitionTablePruning8Test.class, 0, false);
    }

}
