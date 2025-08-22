package com.alibaba.polardbx.qatest.ddl.ttl;

import com.alibaba.polardbx.qatest.ddl.auto.partition.PartitionAutoLoadSqlTestBase;
import org.junit.runners.Parameterized;

import java.util.List;

public class TtlPartitionTableAlterTest extends PartitionAutoLoadSqlTestBase {
    public TtlPartitionTableAlterTest(AutoLoadSqlTestCaseParams parameter) {
        super(parameter);
    }

    @Parameterized.Parameters(name = "{index}: SubTestCase {0}")
    public static List<AutoLoadSqlTestCaseParams> parameters() {
        return getParameters(TtlPartitionTableAlterTest.class, 0, false);
    }
}
