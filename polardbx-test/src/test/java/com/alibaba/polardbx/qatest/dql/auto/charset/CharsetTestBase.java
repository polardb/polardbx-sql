package com.alibaba.polardbx.qatest.dql.auto.charset;

import com.alibaba.polardbx.qatest.AutoReadBaseTestCase;
import com.alibaba.polardbx.qatest.columnar.dql.ColumnarUtils;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import org.junit.After;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeBatchOnMysqlAndTddl;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectContentSameAssert;
import static com.alibaba.polardbx.qatest.validator.DataValidator.selectOrderAssert;

public class CharsetTestBase extends AutoReadBaseTestCase {
    protected static final String COL_UTF8MB4_GENERAL_CI = "v_utf8mb4_general_ci";
    protected static final String COL_UTF8MB4_BIN = "v_utf8mb4_bin";
    protected static final String COL_UTF8MB4_UNICODE_CI = "v_utf8mb4_unicode_ci";
    protected static final String COL_UTF8MB4_UNICODE_520_CI = "v_utf8mb4_unicode_520_ci";
    protected static final String COL_UTF8_GENERAL_CI = "v_utf8_general_ci";
    protected static final String COL_UTF8_BIN = "v_utf8_bin";
    protected static final String COL_UTF8_UNICODE_CI = "v_utf8_unicode_ci";
    protected static final String COL_LATIN1_GENERAL_CS = "v_latin1_general_cs";
    protected static final String COL_LATIN1_GENERAL_CI = "v_latin1_general_ci";
    protected static final String COL_LATIN1_BIN = "v_latin1_bin";
    protected static final String COL_LATIN1_SWEDISH_CI = "v_latin1_swedish_ci";
    protected static final String COL_LATIN1_GERMAN1_CI = "v_latin1_german1_ci";
    protected static final String COL_LATIN1_SPANISH_CI = "v_latin1_spanish_ci";
    protected static final String COL_LATIN1_DANISH_CI = "v_latin1_danish_ci";
    protected static final String COL_ASCII_BIN = "v_ascii_bin";
    protected static final String COL_ASCII_GENERAL_CI = "v_ascii_general_ci";
    protected static final String COL_BINARY = "v_binary";
    protected static final String COL_UTF16_GENERAL_CI = "v_utf16_general_ci";
    protected static final String COL_UTF16_BIN = "v_utf16_bin";
    protected static final String COL_UTF16_UNICODE_CI = "v_utf16_unicode_ci";
    protected static final String COL_GBK_CHINESE_CI = "v_gbk_chinese_ci";
    protected static final String COL_GBK_BIN = "v_gbk_bin";
    protected static final String COL_GB18030_CHINESE_CI = "v_gb18030_chinese_ci";
    protected static final String COL_GB18030_BIN = "v_gb18030_bin";
    protected static final String COL_GB18030_UNICODE_520_CI = "v_gb18030_unicode_520_ci";

    protected static final int STRING_SIZE = 1 << 10;
    protected static final int CHARACTER_SIZE = 5;
    protected String table;
    protected String suffix;
    protected static final String TABLE_PREFIX = "collation_test";
    protected static final List<String[]> PARAMETERS = Arrays.asList(
        new String[] {TABLE_PREFIX + "_one_db_one_tb", ""},
        new String[] {TABLE_PREFIX + "_one_db_multi_tb", "tbpartition by hash(`pk`) tbpartitions 4"},
        new String[] {TABLE_PREFIX + "_multi_db_one_tb", "dbpartition by hash(`pk`)"},
        new String[] {
            TABLE_PREFIX + "_multi_db_multi_tb", "dbpartition by hash(`pk`) tbpartition by hash(`pk`) tbpartitions 4"},
        new String[] {TABLE_PREFIX + "_broadcast", "broadcast"}
    );

    protected static final List<String[]> PARAMETERS_FOR_PART_TBL = Arrays.asList(
        new String[] {TABLE_PREFIX + "_one_db_one_tb", "single"},
        new String[] {TABLE_PREFIX + "_multi_db_one_tb", "partition by key(`pk`) partitions 8"},
        new String[] {TABLE_PREFIX + "_broadcast", "broadcast"}
    );

    protected static final String SET_NAMES_FORMAT = "set names %s";
    protected static final String CREATE_TABLE = "create table if not exists %s (\n"
        + "    pk INT UNSIGNED AUTO_INCREMENT NOT NULL PRIMARY KEY,\n"
        + "    v_default varchar(255) not null default 'abc',\n"
        + "    v_utf8mb4 varchar(255) character set utf8mb4 not null default 'abc',\n"
        + "    v_utf8mb4_general_ci varchar(255) character set utf8mb4 collate utf8mb4_general_ci not null default 'abc',\n"
        + "    v_utf8mb4_unicode_ci varchar(255) character set utf8mb4 collate utf8mb4_unicode_ci not null default 'abc',\n"
        + "    v_utf8mb4_unicode_520_ci varchar(255) character set utf8mb4 collate utf8mb4_unicode_520_ci not null default 'abc',\n"
        + "    v_utf8mb4_bin varchar(255) character set utf8mb4 collate utf8mb4_bin not null default 'abc',\n"
        + "    v_binary varchar(255) character set binary not null default 'abc',\n"
        + "    v_ascii_bin varchar(255) character set ascii collate ascii_bin not null default 'abc',\n"
        + "    v_ascii_general_ci varchar(255) character set ascii collate ascii_general_ci not null default 'abc',\n"
        + "    v_ascii varchar(255) character set ascii not null default 'abc',\n"
        + "    v_utf16 varchar(255) character set utf16 not null default 'abc',\n"
        + "    v_utf16_bin varchar(255) character set utf16 collate utf16_bin not null default 'abc',\n"
        + "    v_utf16_general_ci varchar(255) character set utf16 collate utf16_general_ci not null default 'abc',\n"
        + "    v_utf16_unicode_ci varchar(255) character set utf16 collate utf16_unicode_ci not null default 'abc',\n"
        + "    v_utf8 varchar(255) character set utf8 not null default 'abc',\n"
        + "    v_utf8_bin varchar(255) character set utf8 collate utf8_bin not null default 'abc',\n"
        + "    v_utf8_general_ci varchar(255) character set utf8 collate utf8_general_ci not null default 'abc',\n"
        + "    v_utf8_unicode_ci varchar(255) character set utf8 collate utf8_unicode_ci not null default 'abc',\n"
        + "    v_utf16le varchar(255) character set utf16le not null default 'abc',\n"
        + "    v_utf16le_bin varchar(255) character set utf16le collate utf16le_bin not null default 'abc',\n"
        + "    v_utf16le_general_ci varchar(255) character set utf16le collate utf16le_general_ci not null default 'abc',\n"
        + "    v_latin1 varchar(255) character set latin1 not null default 'abc',\n"
        + "    v_latin1_swedish_ci varchar(255) character set latin1 collate latin1_swedish_ci not null default 'abc',\n"
        + "    v_latin1_german1_ci varchar(255) character set latin1 collate latin1_german1_ci not null default 'abc',\n"
        + "    v_latin1_danish_ci varchar(255) character set latin1 collate latin1_danish_ci not null default 'abc',\n"
        + "    v_latin1_german2_ci varchar(255) character set latin1 collate latin1_german2_ci not null default 'abc',\n"
        + "    v_latin1_bin varchar(255) character set latin1 collate latin1_bin not null default 'abc',\n"
        + "    v_latin1_general_ci varchar(255) character set latin1 collate latin1_general_ci not null default 'abc',\n"
        + "    v_latin1_general_cs varchar(255) character set latin1 collate latin1_general_cs not null default 'abc',\n"
        + "    v_latin1_spanish_ci varchar(255) character set latin1 collate latin1_spanish_ci not null default 'abc',\n"
        + "    v_gbk varchar(255) character set gbk not null default 'abc',\n"
        + "    v_gbk_chinese_ci varchar(255) character set gbk collate gbk_chinese_ci not null default 'abc',\n"
        + "    v_gbk_bin varchar(255) character set gbk collate gbk_bin not null default 'abc',\n"
        + "    v_gb18030 varchar(255) character set gb18030 not null default 'abc',\n"
        + "    v_gb18030_chinese_ci varchar(255) character set gb18030 collate gb18030_chinese_ci not null default 'abc',\n"
        + "    v_gb18030_bin varchar(255) character set gb18030 collate gb18030_bin not null default 'abc',\n"
        + "    v_gb18030_unicode_520_ci varchar(255) character set gb18030 collate gb18030_unicode_520_ci not null default 'abc',\n"
        + "    v_big5 varchar(255) character set big5 not null default 'abc',\n"
        + "    v_big5_chinese_ci varchar(255) character set big5 collate big5_chinese_ci not null default 'abc',\n"
        + "    v_big5_bin varchar(255) character set big5 collate big5_bin not null default 'abc'\n"
        + ") %s";

    protected static final String INSERT_SQL_FORMAT = "insert into %s (%s) values (?)";

    protected static final String INSERT_TWO_COL_SQL_FORMAT = "insert into %s (%s, %s) values (?, ?)";

    protected static final String ORDER_BY_SQL_FORMAT =
        "/*+TDDL:ENABLE_PUSH_SORT=false*/select hex(%s) from %s order by %s, hex(%s)";

    protected static final String DISTINCT_SQL_FORMAT =
        "/*+TDDL:ENABLE_PUSH_AGG=false*/select count(distinct %s ) from %s";

    protected static final String JOIN_SQL_FORMAT =
        "/*+TDDL:ENABLE_PUSH_JOIN=false*/select hex(a.%s), hex(b.%s) from %s a inner join %s b where a.%s = b.%s";

    protected static final String INSTR_SQL_FORMAT =
        "/*+TDDL:ENABLE_PUSH_PROJECT=false*/select %s from %s order by %s, hex(%s)";

    @Before
    public void preparMySQLTable() {
        String mysqlSql = String.format(CREATE_TABLE, table, "");
        JdbcUtil.executeSuccess(mysqlConnection, mysqlSql);
        JdbcUtil.executeSuccess(mysqlConnection, String.format("truncate table %s", table));

        JdbcUtil.executeSuccess(mysqlConnection, String.format(SET_NAMES_FORMAT, "utf8mb4"));
    }

    @Before
    public void preparePolarDBXTable() {
        String tddlSql = String.format(CREATE_TABLE, table, suffix);
        JdbcUtil.executeAndRetry(tddlConnection, tddlSql, 5);
        JdbcUtil.executeSuccess(tddlConnection, String.format("truncate table %s", table));

        JdbcUtil.executeSuccess(tddlConnection, String.format(SET_NAMES_FORMAT, "utf8mb4"));
    }

    @After
    public void afterTable() {
        JdbcUtil.dropTable(tddlConnection, table);
        JdbcUtil.dropTable(mysqlConnection, table);
    }

    protected void insertStrings(List<byte[]> bytesList, String col) {
        final String insertSql = String.format(INSERT_SQL_FORMAT, table, col);
        List params = bytesList.stream()
            .map(bs -> new String(bs))
            .map(Collections::singletonList)
            .collect(Collectors.toList());

        executeBatchOnMysqlAndTddl(mysqlConnection, tddlConnection, insertSql, params);
    }

    protected void insertStrings(List<byte[]> bytesList, String col, String col2) {
        final String insertSql = String.format(INSERT_TWO_COL_SQL_FORMAT, table, col, col2);
        List params = bytesList.stream()
            .map(bs -> new String(bs))
            .map(s -> Arrays.asList(s, s))
            .collect(Collectors.toList());
        executeBatchOnMysqlAndTddl(mysqlConnection, tddlConnection, insertSql, params);
    }

    protected void testOrderBy(List<byte[]> bytesList, String col) {
        insertStrings(bytesList, col);
        createColumnarIndexIfNeed(table, col);
        String selectSql = String.format(ORDER_BY_SQL_FORMAT, col, table, col, col);
        selectOrderAssert(selectSql, null, mysqlConnection, tddlConnection);
    }

    protected void testDistinct(List<byte[]> bytesList, String col) {
        insertStrings(bytesList, col);
        createColumnarIndexIfNeed(table, col);
        String selectSql = String.format(DISTINCT_SQL_FORMAT, col, table);
        selectContentSameAssert(selectSql, null, mysqlConnection, tddlConnection);
    }

    protected void testJoin(List<byte[]> bytesList, String col) {
        if (isMySQL80()) {
            return; // this test only for mysql57
        }
        insertStrings(bytesList, col);
        createColumnarIndexIfNeed(table, col);
        String selectSql = String.format(JOIN_SQL_FORMAT, col, col, table, table, col, col);
        selectContentSameAssert(selectSql, null, mysqlConnection, tddlConnection);
        if (PropertiesUtil.columnarMode()) {
            ColumnarUtils.dropColumnarIndex(tddlConnection, "col_" + table, table);
        }
    }

    protected void testJoin(List<byte[]> bytesList, String col, String col2) {
        if (isMySQL80()) {
            return; // this test only for mysql57
        }
        JdbcUtil.executeSuccess(mysqlConnection, String.format("truncate table %s", table));
        JdbcUtil.executeSuccess(tddlConnection, String.format("truncate table %s", table));
        insertStrings(bytesList, col, col2);
        createColumnarIndexIfNeed(table, col);
        String selectSql = String.format(JOIN_SQL_FORMAT, col, col2, table, table, col, col2);
        selectContentSameAssert(selectSql, null, mysqlConnection, tddlConnection);
        if (PropertiesUtil.columnarMode()) {
            ColumnarUtils.dropColumnarIndex(tddlConnection, "col_" + table, table);
        }
    }

    protected void testInstr(List<byte[]> bytesList, List<byte[]> subStrs, String col, String charset,
                             String collation) {
        StringBuilder sb = new StringBuilder();
        for (byte[] bs : subStrs) {
            String literal = CharsetTestUtils.toLiteral(charset, collation, bs);
            sb.append("instr(");
            sb.append(col);
            sb.append(", ");
            sb.append(literal);
            sb.append("),");
        }
        String instrs = sb.substring(0, sb.length() - 1);
        insertStrings(bytesList, col);
        createColumnarIndexIfNeed(table, col);
        String selectSql = String.format(INSTR_SQL_FORMAT, instrs, table, col, col);

        selectContentSameAssert(selectSql, null, mysqlConnection, tddlConnection);
    }

    protected void testInstrRaw(List<byte[]> bytesList, List<byte[]> subStrs, String col) {
        StringBuilder sb = new StringBuilder();
        for (byte[] bs : subStrs) {
            sb.append("instr(");
            sb.append(col);
            sb.append(", '");
            sb.append((new String(bs)).replace("\\", "\\\\").replace("'", "\\'"));
            sb.append("'),");
        }
        String instrs = sb.substring(0, sb.length() - 1);
        insertStrings(bytesList, col);
        createColumnarIndexIfNeed(table, col);
        String selectSql = String.format(INSTR_SQL_FORMAT, instrs, table, col, col);

        selectContentSameAssert(selectSql, null, mysqlConnection, tddlConnection);
    }

    private void createColumnarIndexIfNeed(String table, String col) {
        if (!PropertiesUtil.columnarMode()) {
            return;
        }
        ColumnarUtils.createColumnarIndex(tddlConnection, "col_" + table, table, col, col, 4);
    }
}
