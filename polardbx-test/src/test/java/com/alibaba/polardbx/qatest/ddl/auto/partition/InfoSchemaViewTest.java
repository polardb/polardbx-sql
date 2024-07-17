package com.alibaba.polardbx.qatest.ddl.auto.partition;

import net.jcip.annotations.NotThreadSafe;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;

/**
 * @author chenghui.lch
 */
@RunWith(value = Parameterized.class)
@NotThreadSafe
public class InfoSchemaViewTest extends PartitionAutoLoadSqlTestBase {

    public InfoSchemaViewTest(AutoLoadSqlTestCaseParams parameter) {
        super(parameter);
    }

    @Parameterized.Parameters(name = "{index}: SubTestCase {0}")
    public static List<AutoLoadSqlTestCaseParams> parameters() {
        return getParameters(InfoSchemaViewTest.class, 0, false);
    }

}
