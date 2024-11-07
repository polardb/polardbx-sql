package com.alibaba.polardbx.qatest.ddl.auto.dal;

import com.alibaba.polardbx.qatest.ddl.auto.partition.PartitionAutoLoadSqlTestBase;
import net.jcip.annotations.NotThreadSafe;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;

@RunWith(value = Parameterized.class)
@NotThreadSafe
public class TimeZoneRoutingTest extends PartitionAutoLoadSqlTestBase {

    public TimeZoneRoutingTest(AutoLoadSqlTestCaseParams parameter) {
        super(parameter);
    }

    @Parameterized.Parameters(name = "{index}: SubTestCase {0}")
    public static List<AutoLoadSqlTestCaseParams> parameters() {
        return getParameters(TimeZoneRoutingTest.class, 0, false);
    }

}
