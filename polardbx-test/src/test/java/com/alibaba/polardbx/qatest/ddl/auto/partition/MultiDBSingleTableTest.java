package com.alibaba.polardbx.qatest.ddl.auto.partition;

import com.alibaba.polardbx.qatest.ddl.auto.locality.LocalityTestCaseUtils.LocalityTestUtils;
import com.alibaba.polardbx.server.util.StringUtil;
import org.junit.runners.Parameterized;

import java.util.List;
import java.util.regex.Pattern;

public class MultiDBSingleTableTest extends PartitionAutoLoadSqlTestBase {

    private String targetStorage = null;
    private String substitute = "$dn1";

    public MultiDBSingleTableTest(AutoLoadSqlTestCaseParams parameter) {
        super(parameter);
    }

    @Parameterized.Parameters(name = "{index}: SubTestCase {0}")
    public static List<AutoLoadSqlTestCaseParams> parameters() {

        return getParameters(MultiDBSingleTableTest.class, 0, false);
    }

    @Override
    protected String applySubstitute(String sql) {
        if (targetStorage == null) {
            targetStorage = LocalityTestUtils.getDatanodes(getPolardbxConnection()).get(0);
        }
        if (!StringUtil.isEmpty(sql)) {
            sql = sql.replaceAll(Pattern.quote(substitute), targetStorage);
        }
        return sql;
    }
}
