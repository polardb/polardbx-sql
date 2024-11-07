package com.alibaba.polardbx.qatest.ddl.auto.partition;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;

/**
 * @author chenghui.lch
 */
@RunWith(value = Parameterized.class)
public class PartitionTablePruning4Test extends PartitionAutoLoadSqlTestBase {

    public PartitionTablePruning4Test(AutoLoadSqlTestCaseParams parameter) {
        super(parameter);
    }

    @Parameterized.Parameters(name = "{index}: SubTestCase {0}")
    public static List<AutoLoadSqlTestCaseParams> parameters() {
        return getParameters(PartitionTablePruning4Test.class, 0, false);
    }

}
