package com.alibaba.polardbx.qatest.ddl.auto.partition;

import net.jcip.annotations.NotThreadSafe;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;

/**
 * @author luoyanxin
 */
@RunWith(value = Parameterized.class)
@NotThreadSafe
public class PartitionTableTtlTest extends PartitionAutoLoadSqlTestBase {

    public PartitionTableTtlTest(AutoLoadSqlTestCaseParams parameter) {
        super(parameter);
    }

    @Parameterized.Parameters(name = "{index}: SubTestCase {0}")
    public static List<AutoLoadSqlTestCaseParams> parameters() {
        return getParameters(PartitionTableTtlTest.class, 0, false);
    }

}
