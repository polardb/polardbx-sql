package com.alibaba.polardbx.qatest.dql.auto.charset;

import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import com.google.common.collect.ImmutableMultimap;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.List;

import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;

public class CollationCompareTest extends CharsetTestBase {
    @Parameterized.Parameters(name = "{index}:table0={0},table1={1}")
    public static List<String[]> prepare() {
        return PARAMETERS_FOR_PART_TBL;
    }

    public CollationCompareTest(String table, String suffix) {
        this.table = randomTableName(table, 4);
        this.suffix = suffix;
    }

    private static final String COMPARE_CASE_FORMAT = "select _%s'abc' collate %s %s _%s'AbC' collate %s";
    private static final String COMPARE_SPACE_FORMAT = "select _%s'abc  ' collate %s %s _%s'abc ' collate %s";
    private static final String[] COMPARE_OPERATORS = {"=", ">", ">=", "<", "<=", "<=>", "<>"};
    private static final ImmutableMultimap<String, String> MAP = ImmutableMultimap.<String, String>builder()
        .put("utf8mb4", "utf8mb4_general_ci")
        .put("utf8mb4", "utf8mb4_unicode_ci")
        .put("utf8mb4", "utf8mb4_unicode_520_ci")
        .put("utf8mb4", "utf8mb4_bin")
        .put("ascii", "ascii_bin")
        .put("ascii", "ascii_general_ci")
        .put("utf8", "utf8_bin")
        .put("utf8", "utf8_general_ci")
        .put("utf8", "utf8_unicode_ci")
        .put("latin1", "latin1_swedish_ci")
        .put("latin1", "latin1_german1_ci")
        .put("latin1", "latin1_danish_ci")
        .put("latin1", "latin1_bin")
        .put("latin1", "latin1_general_ci")
        .put("latin1", "latin1_general_cs")
        .put("latin1", "latin1_spanish_ci")
        .put("gbk", "gbk_chinese_ci")
        .put("gbk", "gbk_bin")
        .put("gb18030", "gb18030_chinese_ci")
        .put("gb18030", "gb18030_bin")
        .put("gb18030", "gb18030_unicode_520_ci")
        .build();

    @Test
    public void testCase() {
        if (PropertiesUtil.usePrepare()) {
            // current prepare mode does not support collation
            return;
        }
        MAP.forEach(
            (charset, collation) -> {
                for (String op : COMPARE_OPERATORS) {
                    String sql = String.format(COMPARE_CASE_FORMAT, charset, collation, op, charset, collation);
                    selectContentSameAssert(sql, null, tddlConnection, mysqlConnection);
                }
            }
        );
    }

    @Test
    public void testSpace() {
        MAP.forEach(
            (charset, collation) -> {
                for (String op : COMPARE_OPERATORS) {
                    String sql = String.format(COMPARE_SPACE_FORMAT, charset, collation, op, charset, collation);
                    selectContentSameAssert(sql, null, tddlConnection, mysqlConnection);
                }
            }
        );
    }

}
