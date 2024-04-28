package com.alibaba.polardbx.qatest.columnar.gms;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.runners.Parameterized;

import java.util.List;

@Ignore
public class CreateColumnarIndexColumnarManagerTest extends BaseColumnarManagerTest {
    public CreateColumnarIndexColumnarManagerTest(String caseName, String schemaName, List<String> metaDbSqls) {
        super(caseName, schemaName, metaDbSqls);
    }

    @Before
    public void before() {
        clearColumnarMeta();
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return loadTestCases(CreateColumnarIndexColumnarManagerTest.class);
    }
}
