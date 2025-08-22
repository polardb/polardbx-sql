package com.alibaba.polardbx.qatest.ddl.auto.partition;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;

/**
 * @author chenghui.lch
 */
@RunWith(value = Parameterized.class)
public class PartitionTablePruning7Test extends PartitionAutoLoadSqlTestBase {

    public PartitionTablePruning7Test(AutoLoadSqlTestCaseParams parameter) {
        super(parameter);
    }

    @Parameterized.Parameters(name = "{index}: SubTestCase {0}")
    public static List<AutoLoadSqlTestCaseParams> parameters() {
        return getParameters(PartitionTablePruning7Test.class, 0, false);
    }

}
