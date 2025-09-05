package com.alibaba.polardbx.qatest.ddl.auto.partition;

import com.alibaba.polardbx.qatest.ddl.datamigration.locality.LocalityTestCaseUtils.LocalityTestUtils;
import com.alibaba.polardbx.server.util.StringUtil;
import org.junit.runners.Parameterized;

import java.util.List;
import java.util.regex.Pattern;

public class OmcLocalityTest extends PartitionAutoLoadSqlTestBase {

    private String targetStorage = null;
    private String substitute = "$dn1";

    public OmcLocalityTest(AutoLoadSqlTestCaseParams parameter) {
        super(parameter);
        parameter.ignoreLocality = true;
        parameter.supportAutoPart = true;
    }

    @Parameterized.Parameters(name = "{index}: SubTestCase {0}")
    public static List<AutoLoadSqlTestCaseParams> parameters() {

        return getParameters(OmcLocalityTest.class, 0, false);
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