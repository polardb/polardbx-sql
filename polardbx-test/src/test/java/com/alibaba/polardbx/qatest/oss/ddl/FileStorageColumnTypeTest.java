package com.alibaba.polardbx.qatest.oss.ddl;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.oss.utils.FileStorageTestUtil;
import com.alibaba.polardbx.qatest.oss.utils.FullTypeSeparatedTestUtil;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import com.alibaba.polardbx.qatest.validator.DataValidator;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang.StringUtils;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.qatest.oss.utils.FullTypeTestUtil.C_BIGINT_1;
import static com.alibaba.polardbx.qatest.oss.utils.FullTypeTestUtil.C_BIGINT_64;
import static com.alibaba.polardbx.qatest.oss.utils.FullTypeTestUtil.C_BIGINT_64_UN;
import static com.alibaba.polardbx.qatest.oss.utils.FullTypeTestUtil.C_INT_1;
import static com.alibaba.polardbx.qatest.oss.utils.FullTypeTestUtil.C_INT_32;
import static com.alibaba.polardbx.qatest.oss.utils.FullTypeTestUtil.C_INT_32_UN;
import static com.alibaba.polardbx.qatest.oss.utils.FullTypeTestUtil.C_MEDIUMINT_1;
import static com.alibaba.polardbx.qatest.oss.utils.FullTypeTestUtil.C_MEDIUMINT_24;
import static com.alibaba.polardbx.qatest.oss.utils.FullTypeTestUtil.C_MEDIUMINT_24_UN;
import static com.alibaba.polardbx.qatest.oss.utils.FullTypeTestUtil.C_SMALLINT_1;
import static com.alibaba.polardbx.qatest.oss.utils.FullTypeTestUtil.C_SMALLINT_16;
import static com.alibaba.polardbx.qatest.oss.utils.FullTypeTestUtil.C_SMALLINT_16_UN;
import static com.alibaba.polardbx.qatest.oss.utils.FullTypeTestUtil.C_TINYINT_1;
import static com.alibaba.polardbx.qatest.oss.utils.FullTypeTestUtil.C_TINYINT_1_UN;
import static com.alibaba.polardbx.qatest.oss.utils.FullTypeTestUtil.C_TINYINT_4;
import static com.alibaba.polardbx.qatest.oss.utils.FullTypeTestUtil.C_TINYINT_4_UN;
import static com.alibaba.polardbx.qatest.oss.utils.FullTypeTestUtil.C_TINYINT_8;
import static com.alibaba.polardbx.qatest.oss.utils.FullTypeTestUtil.C_TINYINT_8_UN;

public class FileStorageColumnTypeTest extends DDLBaseNewDBTestCase {

    protected static final Engine engine = PropertiesUtil.engine();

    protected final String sourceCol;

    protected final String targetCol;

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    protected static final List<String> SIGNED_INT_CONV = ImmutableList.of(
        C_TINYINT_1,
        C_TINYINT_4,
        C_TINYINT_8,
        C_SMALLINT_1,
        C_SMALLINT_16,
        C_MEDIUMINT_1,
        C_MEDIUMINT_24,
        C_INT_1,
        C_INT_32,
        C_BIGINT_1,
        C_BIGINT_64
    );

    protected static final List<String> UNSIGNED_INT_CONV = ImmutableList.of(
        C_TINYINT_1_UN,
        C_TINYINT_4_UN,
        C_TINYINT_8_UN,
        C_SMALLINT_16_UN,
        C_MEDIUMINT_24_UN,
        C_INT_32_UN,
        C_BIGINT_64_UN
    );

    /**
     * TIMESTAMP a range of '1970-01-01 00:00:01.000000' to '2038-01-19 03:14:07.999999'.
     * DATE The supported range is '1000-01-01' to '9999-12-31'.
     * DATETIME The supported range is '1000-01-01 00:00:00.000000' to '9999-12-31 23:59:59.999999'.
     */
    protected static final List<Pair<String, String>> PAIR_CONV = ImmutableList.of(
        new Pair<>("c_date", "c_datetime"),
        new Pair<>("c_datetime", "c_date"),
        new Pair<>("c_timestamp", "c_datetime"),
        new Pair<>("c_timestamp_3", "c_datetime_3"),
        new Pair<>("c_timestamp_3", "c_datetime_6")
    );

    protected static final List<String> TIMESTAMP_CONV = ImmutableList.of(
        "c_timestamp",
        "c_timestamp_1",
        "c_timestamp_3",
        "c_timestamp_6"
    );

    private static final List<String> DATETIME_CONV = ImmutableList.of(
        "c_datetime",
        "c_datetime_1",
        "c_datetime_3",
        "c_datetime_6"
    );

    protected static final int BATCH_SIZE = 1 << 10;

    protected static final String INSERT_SQL_FORMAT = "insert into %s (%s) values (?)";

    protected final Map<String, FullTypeSeparatedTestUtil.TypeItem> colItemMap =
        Arrays.stream(FullTypeSeparatedTestUtil.TypeItem.values())
            .collect(
                Collectors.toMap(FullTypeSeparatedTestUtil.TypeItem::getKey, c -> c));

    public FileStorageColumnTypeTest(String sourceCol, String targetCol, String crossSchema) {
        this.sourceCol = sourceCol;
        this.targetCol = targetCol;
        this.crossSchema = Boolean.parseBoolean(crossSchema);
    }

    @Parameterized.Parameters(name = "{index}:source={0} target={1} cross={2}")
    public static List<String[]> prepareData() {
        List<String[]> para = new ArrayList<>();
        List<List<String>> testCases = ImmutableList.of(
            //BIT_CONV,
            SIGNED_INT_CONV,
            UNSIGNED_INT_CONV
            //TIMESTAMP_CONV,
            //DATETIME_CONV
        );
        List<String> cross = ImmutableList.of("false", "true");
        for (String crossDB : cross) {
            for (List<String> columns : testCases) {
                for (int i = 0; i < columns.size(); i++) {
                    for (int j = i + 1; j < columns.size(); j++) {
                        if (StringUtils.equals(columns.get(i), columns.get(j))) {
                            continue;
                        }
                        para.add(new String[] {columns.get(i), columns.get(j), crossDB});
                    }
                }
            }
            for (Pair<String, String> pair : PAIR_CONV) {
                //para.add(new String[] {pair.getKey(), pair.getValue(), crossDB});
            }
        }

        return para;
    }

    protected Connection getInnoConn() {
        return tddlConnection;
    }

    protected String getInnoSchema() {
        return tddlDatabase1;
    }

    protected Connection getOssConn() {
        return crossSchema ? getTddlConnection2() : getTddlConnection1();
    }

    @Test
    public void testFullTypeByColumn() {
        try {
            // create table
            FullTypeSeparatedTestUtil.TypeItem typeItem = colItemMap.get(sourceCol);
            String innoTable = FullTypeSeparatedTestUtil.tableNameByColumn(sourceCol);
            String ossTable = "oss_" + innoTable;

            dropTableIfExists(getInnoConn(), innoTable);
            dropTableIfExists(getOssConn(), ossTable);

            // insert data
            // gen data by column-specific type item.
            FullTypeSeparatedTestUtil.createInnodbTableWithData(getInnoConn(), typeItem);
            // clone an oss-table from innodb-table.
            FileStorageTestUtil.createLoadingTable(getOssConn(), ossTable, getInnoSchema(), innoTable, engine);

            // check before ddl
            checkAgg(innoTable, ossTable, sourceCol);
            checkFilter(innoTable, ossTable, sourceCol);

            // ddl
            StringBuilder ddl = new StringBuilder("alter table %s modify column ");
            String columnDef =
                FullTypeSeparatedTestUtil.allColumnDefinitions.get(targetCol).replace(targetCol, sourceCol)
                    .replace(",\n", "");
            ddl.append(columnDef);
            performDdl(getInnoConn(), ddl.toString(), innoTable);
            performDdl(getOssConn(), ddl.toString(), ossTable);

            // check after ddl
            checkAgg(innoTable, ossTable, sourceCol);
            checkFilter(innoTable, ossTable, sourceCol);
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }

    protected void performDdl(Connection conn, String ddl, String table) {
        String sqlMode = JdbcUtil.getSqlMode(conn);
        setSqlMode("", conn);
        JdbcUtil.executeSuccess(conn, String.format(ddl, table));
        setSqlMode(sqlMode, conn);
    }

    protected void checkAgg(String innodbTable, String ossTable, String column) {
        String innoSql = String.format("select check_sum(%s) from %s", column, innodbTable);
        String ossSql = String.format("select check_sum(%s) from %s", column, ossTable);
        DataValidator.selectContentSameAssertWithDiffSql(ossSql, innoSql, null,
            getInnoConn(), getOssConn(), true, false, true);

        innoSql = String.format("select count(%s) from %s", column, innodbTable);
        ossSql = String.format("select count(%s) from %s", column, ossTable);
        DataValidator.selectContentSameAssertWithDiffSql(ossSql, innoSql, null,
            getInnoConn(), getOssConn(), true, false, true);
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

        // ignore big_integer,float,double,bit
        if (goal == null || goal instanceof BigInteger || goal instanceof byte[]) {
            return;
        }
        List<Object> para = ImmutableList.of(goal);

        boolean checkLike = true;
        checkLike = !(goal instanceof Boolean);
        if (checkLike) {
            innoSql = String.format("select check_sum(%s) from %s where %s like ?", column, innodbTable, column);
            ossSql = String.format("select check_sum(%s) from %s where %s like ?", column, ossTable, column);
            DataValidator.selectContentSameAssertWithDiffSql(ossSql, innoSql, para,
                getInnoConn(), getOssConn(), true, false, true);

        }
        // ignore big_integer,float,double,bit
        if (goal instanceof Float || goal instanceof Double) {
            return;
        }
        innoSql =
            String.format("select check_sum(%s) from %s where %s = ?", column, innodbTable, column);
        ossSql = String.format("select check_sum(%s) from %s where %s = ?", column, ossTable, column);
        DataValidator.selectContentSameAssertWithDiffSql(ossSql, innoSql, para,
            getInnoConn(), getOssConn(), true, false, true);
    }
}
