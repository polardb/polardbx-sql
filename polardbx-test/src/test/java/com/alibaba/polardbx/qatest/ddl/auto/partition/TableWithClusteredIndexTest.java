package com.alibaba.polardbx.qatest.ddl.auto.partition;

import org.junit.runners.Parameterized;

import java.util.List;

public class TableWithClusteredIndexTest extends PartitionAutoLoadSqlTestBase {

    public TableWithClusteredIndexTest(AutoLoadSqlTestCaseParams parameter) {
        super(parameter);
    }

    @Parameterized.Parameters(name = "{index}: SubTestCase {0}")
    public static List<AutoLoadSqlTestCaseParams> parameters() {

        return getParameters(TableWithClusteredIndexTest.class, 3, true);
    }

}