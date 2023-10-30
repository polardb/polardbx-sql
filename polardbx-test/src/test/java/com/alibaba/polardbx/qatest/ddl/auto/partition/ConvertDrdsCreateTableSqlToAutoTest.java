package com.alibaba.polardbx.qatest.ddl.auto.partition;

import org.junit.runners.Parameterized;

import java.util.List;

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */

public class ConvertDrdsCreateTableSqlToAutoTest extends PartitionAutoLoadSqlTestBase {
    public ConvertDrdsCreateTableSqlToAutoTest(AutoLoadSqlTestCaseParams parameter) {
        super(parameter);
    }

    @Parameterized.Parameters(name = "{index}: SubTestCase {0}")
    public static List<AutoLoadSqlTestCaseParams> parameters() {
        return getParameters(ConvertDrdsCreateTableSqlToAutoTest.class, 0, false);
    }
}
