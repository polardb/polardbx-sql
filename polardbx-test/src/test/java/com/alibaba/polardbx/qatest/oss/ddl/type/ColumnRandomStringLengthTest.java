package com.alibaba.polardbx.qatest.oss.ddl.type;

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.qatest.dql.sharding.type.numeric.CastTestUtils;
import com.alibaba.polardbx.qatest.oss.ddl.FileStorageColumnTypeTest;
import com.alibaba.polardbx.qatest.oss.utils.FileStorageTestUtil;
import com.alibaba.polardbx.qatest.oss.utils.FullTypeSeparatedTestUtil;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.validator.DataValidator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ColumnRandomStringLengthTest extends FileStorageColumnTypeTest {

    private static final String C_CHAR_10 = "c_char_10";

    private static final String C_CHAR_30 = "c_char_30";

    private static final String C_VARCHAR_10 = "c_varchar_10";

    private static final String C_VARCHAR_30 = "c_varchar_30";

    private static final String C_VARBINARY_10 = "c_varbinary_10";

    private static final String C_VARBINARY_30 = "c_varbinary_30";

    public static ImmutableMap<String, String> STRING_LEN_COLUMN = ImmutableMap.<String, String>builder()
        .put(C_CHAR_10, "\t`c_char_10` char(10) DEFAULT NULL,\n")
        .put(C_CHAR_30, "\t`c_char_30` char(30) DEFAULT NULL,\n")
        .put(C_VARCHAR_10, "\t`c_varchar_10` varchar(10) DEFAULT NULL,\n")
        .put(C_VARCHAR_30, "\t`c_varchar_30` varchar(30) DEFAULT NULL,\n")
        .put(C_VARBINARY_10, "\t`c_varbinary_10` varbinary(10) DEFAULT NULL,\n")
        .put(C_VARBINARY_30, "\t`c_varbinary_30` varbinary(30) DEFAULT NULL,\n")
        .build();

    protected static final List<Pair<String, String>> PAIR_CONV = ImmutableList.of(
        new Pair<>(C_CHAR_10, C_CHAR_30),
        new Pair<>(C_CHAR_30, C_CHAR_30),
        new Pair<>(C_VARCHAR_10, C_VARCHAR_30)
    );

    public ColumnRandomStringLengthTest(String sourceCol, String targetCol) {
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
                    STRING_LEN_COLUMN.get(sourceCol)));

            // gen data by column-specific type item.
            Supplier<Object> generator1 = getGenerator(sourceCol, false);
            // generate insert value & do insert
            List<Object> paramList = IntStream.range(0, 300)
                .mapToObj(i -> generator1.get())
                .collect(Collectors.toList());
            insertData(paramList, innoTable, sourceCol);

            // clone an oss-table from innodb-table.
            FileStorageTestUtil.createLoadingTable(getOssConn(), ossTable, getInnoSchema(), innoTable, engine);

            // check
            checkAgg(innoTable, ossTable, sourceCol);
            checkFilter(innoTable, ossTable, sourceCol);
            // ddl
            StringBuilder ddl = new StringBuilder("alter table %s modify column ");
            String columnDef =
                STRING_LEN_COLUMN.get(targetCol).replace(targetCol, sourceCol)
                    .replace(",\n", "");
            ddl.append(columnDef);

            performDdl(getInnoConn(), ddl.toString(), innoTable);
            performDdl(getOssConn(), ddl.toString(), ossTable);
            // check
            checkAgg(innoTable, ossTable, sourceCol);
            checkFilter(innoTable, ossTable, sourceCol);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    protected void insertData(List<Object> paramList, String table, String col1) {
        String insertSql = String.format(INSERT_SQL_FORMAT, table, col1);
        List params = paramList.stream()
            .map(Collections::singletonList)
            .collect(Collectors.toList());

        JdbcUtil.updateDataBatch(getInnoConn(), insertSql, params);
    }

    protected static Supplier<Object> getGenerator(final String col, boolean hasNull) {
        switch (col) {
        case C_CHAR_10:
        case C_VARCHAR_10:
        case C_VARBINARY_10:
            return () -> hasNull && CastTestUtils.useNull() ? null : RandomStringUtils.randomAscii(10);
        case C_CHAR_30:
        case C_VARCHAR_30:
        case C_VARBINARY_30:
            return () -> hasNull && CastTestUtils.useNull() ? null : RandomStringUtils.randomAscii(30);
        default:
            return () -> hasNull && CastTestUtils.useNull() ? null
                : (CastTestUtils.useNatural()
                ? CastTestUtils.randomValidDecimal(10, 0)
                : CastTestUtils.randomStr());
        }
    }

    protected void checkFilter(String innodbTable, String ossTable, String column) {
        String selectSql = "select %s from %s where %s is not null limit 1";

        String innoSql;
        String ossSql;

        ResultSet rs =
            JdbcUtil.executeQuery(String.format(selectSql, column, innodbTable, column), getInnoConn());
        Object goal;
        try {
            rs.next();
            goal = rs.getObject(1);
        } catch (SQLException ignore) {
            return;
        }

        List<Object> para = ImmutableList.of(goal);
        innoSql =
            String.format("select check_sum(%s) from %s where %s = ?", column, innodbTable, column);
        ossSql = String.format("select check_sum(%s) from %s where %s = ?", column, ossTable, column);
        DataValidator.selectContentSameAssertWithDiffSql(ossSql, innoSql, para,
            getInnoConn(), getOssConn(), true, false, true);
    }
}
