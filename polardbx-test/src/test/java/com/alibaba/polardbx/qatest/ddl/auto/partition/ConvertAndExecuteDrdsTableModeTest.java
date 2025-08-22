package com.alibaba.polardbx.qatest.ddl.auto.partition;

import org.junit.runners.Parameterized;

import java.util.List;

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */

public class ConvertAndExecuteDrdsTableModeTest extends PartitionAutoLoadSqlTestBase {
    public ConvertAndExecuteDrdsTableModeTest(AutoLoadSqlTestCaseParams parameter) {
        super(parameter);
    }

    @Parameterized.Parameters(name = "{index}: SubTestCase {0}")
    public static List<AutoLoadSqlTestCaseParams> parameters() {
        List<AutoLoadSqlTestCaseParams> params =
            getParameters(ConvertAndExecuteDrdsTableModeTest.class, 0, false);
        params.forEach(x -> x.setIgnoreAutoIncrement(true));
        return params;
    }
}
