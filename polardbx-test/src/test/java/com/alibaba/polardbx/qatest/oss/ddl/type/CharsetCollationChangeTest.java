package com.alibaba.polardbx.qatest.oss.ddl.type;

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.qatest.dql.sharding.charset.CharsetTestUtils;
import com.alibaba.polardbx.qatest.oss.ddl.FileStorageColumnTypeTest;
import com.alibaba.polardbx.qatest.oss.utils.FileStorageTestUtil;
import com.alibaba.polardbx.qatest.oss.utils.FullTypeSeparatedTestUtil;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.server.util.StringUtil;
import com.google.common.collect.ImmutableMap;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Ignore("to be supported")
public class CharsetCollationChangeTest extends FileStorageColumnTypeTest {

    protected static final String COL_UTF8MB4_GENERAL_CI = "v_utf8mb4_general_ci";
    protected static final String COL_UTF8MB4_BIN = "v_utf8mb4_bin";
    protected static final String COL_UTF8MB4_UNICODE_CI = "v_utf8mb4_unicode_ci";
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
    protected static final String COL_UTF16_GENERAL_CI = "v_utf16_general_ci";
    protected static final String COL_UTF16_BIN = "v_utf16_bin";
    protected static final String COL_GBK_CHINESE_CI = "v_gbk_chinese_ci";
    protected static final String COL_GBK_BIN = "v_gbk_bin";

    public static ImmutableMap<String, String> STRING_CHAR_COLL_COLUMN = ImmutableMap.<String, String>builder()
        .put(COL_UTF8MB4_GENERAL_CI,
            "\t v_utf8mb4_general_ci varchar(255) character set utf8mb4 collate utf8mb4_general_ci not null default 'abc',\n")
        .put(COL_UTF8MB4_BIN,
            "\tv_utf8mb4_bin varchar(255) character set utf8mb4 collate utf8mb4_bin not null default 'abc',\n")
        .put(COL_UTF8MB4_UNICODE_CI,
            "\tv_utf8mb4_unicode_ci varchar(255) character set utf8mb4 collate utf8mb4_unicode_ci not null default 'abc',\n")
        .put(COL_UTF8_GENERAL_CI,
            "\tv_utf8_general_ci varchar(255) character set utf8 collate utf8_general_ci not null default 'abc',\n")
        .put(COL_UTF8_BIN,
            "\tv_utf8_bin varchar(255) character set utf8 collate utf8_bin not null default 'abc',\n")
        .put(COL_UTF8_UNICODE_CI,
            "\tv_utf8_unicode_ci varchar(255) character set utf8 collate utf8_unicode_ci not null default 'abc',\n")
        .put(COL_LATIN1_GENERAL_CS,
            "\tv_latin1_general_cs varchar(255) character set latin1 collate latin1_general_cs not null default 'abc',\n")
        .put(COL_LATIN1_GENERAL_CI,
            "\tv_latin1_general_ci varchar(255) character set latin1 collate latin1_general_ci not null default 'abc',\n")
        .put(COL_LATIN1_BIN,
            "\tv_latin1_bin varchar(255) character set latin1 collate latin1_bin not null default 'abc',\n")
        .put(COL_LATIN1_SWEDISH_CI,
            "\tv_latin1_swedish_ci varchar(255) character set latin1 collate latin1_swedish_ci not null default 'abc',\n")
        .put(COL_LATIN1_GERMAN1_CI,
            "\tv_latin1_german1_ci varchar(255) character set latin1 collate latin1_german1_ci not null default 'abc',\n")
        .put(COL_LATIN1_SPANISH_CI,
            "\tv_latin1_spanish_ci varchar(255) character set latin1 collate latin1_spanish_ci not null default 'abc',\n")
        .put(COL_LATIN1_DANISH_CI,
            "\tv_latin1_danish_ci varchar(255) character set latin1 collate latin1_danish_ci not null default 'abc',\n")
        .put(COL_ASCII_BIN,
            "\tv_ascii_bin varchar(255) character set ascii collate ascii_bin not null default 'abc',\n")
        .put(COL_ASCII_GENERAL_CI,
            "\tv_ascii_general_ci varchar(255) character set ascii collate ascii_general_ci not null default 'abc',\n")
        .put(COL_UTF16_GENERAL_CI,
            "\tv_utf16_general_ci varchar(255) character set utf16 collate utf16_general_ci not null default 'abc',\n")
        .put(COL_UTF16_BIN,
            "\tv_utf16_bin varchar(255) character set utf16 collate utf16_bin not null default 'abc',\n")
        .put(COL_GBK_CHINESE_CI,
            "\tv_gbk_chinese_ci varchar(255) character set gbk collate gbk_chinese_ci not null default 'abc',\n")
        .put(COL_GBK_BIN,
            "\tv_gbk_bin varchar(255) character set gbk collate gbk_bin not null default 'abc',\n")
        .build();

    protected static final List<Pair<String, String>> PAIR_CONV = STRING_CHAR_COLL_COLUMN.keySet().stream()
        .flatMap(x ->
            STRING_CHAR_COLL_COLUMN.keySet().stream().filter(y -> !StringUtil.equals(x, y)).map(y -> new Pair<>(x, y)))
        .collect(Collectors.toList());

    protected static final int STRING_SIZE = 1 << 10;

    protected static final int CHARACTER_SIZE = 5;

    public CharsetCollationChangeTest(String sourceCol, String targetCol) {
        super(sourceCol, targetCol, "false");
    }

    @Parameterized.Parameters(name = "{index}:source={0} target={1}")
    public static List<String[]> prepareData() {
        List<String[]> para = new ArrayList<>();

        for (Pair<String, String> pair : PAIR_CONV) {
            para.add(new String[] {pair.getKey(), pair.getValue()});
        }
        return para;
    }

    @Test
    public void testFullTypeByColumn() {
        try {
            String innoTable = FullTypeSeparatedTestUtil.tableNameByColumn(sourceCol);
            String ossTable = "oss_" + innoTable;

            dropTableIfExists(getInnoConn(), innoTable);
            dropTableIfExists(getOssConn(), ossTable);

            JdbcUtil.executeSuccess(getInnoConn(),
                String.format(FullTypeSeparatedTestUtil.TABLE_DEFINITION_FORMAT, innoTable,
                    STRING_CHAR_COLL_COLUMN.get(sourceCol)));

            // generate insert value by column-specific type item.
            List<byte[]> paramList = StringGenerator(sourceCol);
            // do insert
            insertStrings(paramList, innoTable, sourceCol);

            // clone an oss-table from innodb-table.
            FileStorageTestUtil.createLoadingTable(getOssConn(), ossTable, getInnoSchema(), innoTable, engine);

            // check before ddl
            checkAgg(innoTable, ossTable, sourceCol);
            checkFilter(innoTable, ossTable, sourceCol);

            // ddl
            StringBuilder ddl = new StringBuilder("alter table %s modify column ");
            String columnDef =
                STRING_CHAR_COLL_COLUMN.get(targetCol).replace(targetCol, sourceCol)
                    .replace(",\n", "");
            ddl.append(columnDef);

            performDdl(getOssConn(), ddl.toString(), ossTable);
            performDdl(getInnoConn(), ddl.toString(), innoTable);
            // check after ddl
            checkAgg(innoTable, ossTable, sourceCol);
            checkFilter(innoTable, ossTable, sourceCol);

        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    protected List<byte[]> StringGenerator(String columnName) {
        switch (columnName) {
        case COL_UTF16_GENERAL_CI:
        case COL_UTF16_BIN:
            return CharsetTestUtils.generateUTF16(STRING_SIZE, CHARACTER_SIZE, true);
        case COL_GBK_CHINESE_CI:
        case COL_GBK_BIN:
            return CharsetTestUtils.generateGBKUnicode(STRING_SIZE, CHARACTER_SIZE, true);
        case COL_UTF8MB4_GENERAL_CI:
        case COL_UTF8MB4_BIN:
        case COL_UTF8MB4_UNICODE_CI:
            CharsetTestUtils.generateUTF8MB4(STRING_SIZE, CHARACTER_SIZE, true);
        case COL_UTF8_GENERAL_CI:
        case COL_UTF8_BIN:
        case COL_UTF8_UNICODE_CI:
            CharsetTestUtils.generateUTF8MB3(STRING_SIZE, CHARACTER_SIZE, true);
        default:
            return CharsetTestUtils.generateSimple(STRING_SIZE, CHARACTER_SIZE, true);
        }
    }

    protected void insertStrings(List<byte[]> bytesList, String table, String col) {
        final String insertSql = String.format(INSERT_SQL_FORMAT, table, col);
        List params = bytesList.stream()
            .map(bs -> new String(bs))
            .map(Collections::singletonList)
            .collect(Collectors.toList());

        JdbcUtil.updateDataBatch(getInnoConn(), insertSql, params);
    }
}