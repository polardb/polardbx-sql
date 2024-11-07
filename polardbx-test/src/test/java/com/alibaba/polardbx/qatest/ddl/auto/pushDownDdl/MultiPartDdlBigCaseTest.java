package com.alibaba.polardbx.qatest.ddl.auto.pushDownDdl;

import com.alibaba.polardbx.qatest.ddl.auto.partition.PartitionAutoLoadSqlTestBase;
import org.junit.runners.Parameterized;

import java.util.List;

/**
 * Created by taokun.
 *
 * @author taokun
 */
public class MultiPartDdlBigCaseTest extends PartitionAutoLoadSqlTestBase {
    public MultiPartDdlBigCaseTest(AutoLoadSqlTestCaseParams parameter) {
        super(parameter);
    }

    @Parameterized.Parameters(name = "{index}: SubTestCase {0}")
    public static List<AutoLoadSqlTestCaseParams> parameters() {
        return getParameters(MultiPartDdlBigCaseTest.class, 0, false);
    }
}
