package com.alibaba.polardbx.qatest.oss.ddl;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.oss.utils.FullTypeSeparatedTestUtil;
import com.alibaba.polardbx.qatest.oss.utils.FullTypeTestUtil;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import com.alibaba.polardbx.qatest.util.RandomUtils;
import com.alibaba.polardbx.qatest.validator.DataValidator;
import com.alibaba.polardbx.server.util.StringUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.math.BigInteger;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.Set;

@Ignore
public class FileStorageColumnDDLBaseTest extends DDLBaseNewDBTestCase {

    protected List<LocalDate> dateList;

    protected Queue<String> localPartitionQueue;

    private static final Engine engine = PropertiesUtil.engine();
    private static String TABLE_DEFINITION_FORMAT = "CREATE TABLE `%s` (\n" +
        "\t`id` bigint(20) NOT NULL AUTO_INCREMENT,\n"
        + "gmt_modified DATE NOT NULL,\n"
        + "PRIMARY KEY (id,gmt_modified)\n"
        + ")\n"
        + "PARTITION BY HASH(id)\n"
        + "PARTITIONS 4\n"
        + "LOCAL PARTITION BY RANGE (gmt_modified)\n"
        + "STARTWITH '%s'\n"
        + "INTERVAL 1 MONTH\n"
        + "EXPIRE AFTER 1\n"
        + "PRE ALLOCATE 3\n"
        + "disable schedule";

    private static String TABLE_DEFINITION_FORMAT_AUTO = "CREATE TABLE `%s` (\n" +
        "\t`id` bigint(20) NOT NULL AUTO_INCREMENT,\n"
        + "gmt_modified DATE NOT NULL,\n"
        + "PRIMARY KEY (id,gmt_modified)\n"
        + ")\n"
        + "LOCAL PARTITION BY RANGE (gmt_modified)\n"
        + "STARTWITH '%s'\n"
        + "INTERVAL 1 MONTH\n"
        + "EXPIRE AFTER 1\n"
        + "PRE ALLOCATE 3\n"
        + "disable schedule";

    protected static List<String> allColumns = ImmutableList.of(
        "c_bit_1",
        "c_bit_8",
        "c_bit_16",
        "c_bit_32",
        "c_bit_64",
        "c_tinyint_1",
        "c_tinyint_1_un",
        "c_tinyint_4",
        "c_tinyint_4_un",
        "c_tinyint_8",
        "c_tinyint_8_un",
        "c_smallint_1",
        "c_smallint_16",
        "c_smallint_16_un",
        "c_mediumint_1",
        "c_mediumint_24",
        "c_mediumint_24_un",
        "c_int_1",
        "c_int_32",
        "c_int_32_un",
        "c_bigint_1",
        "c_bigint_64",
        "c_bigint_64_un",
        "c_decimal",
        "c_decimal_pr",
        "c_float",
        "c_float_pr",
        "c_float_un",
        "c_double",
        "c_double_pr",
        "c_double_un",
        "c_date",
        "c_datetime",
        "c_datetime_1",
        "c_datetime_3",
        "c_datetime_6",
        "c_timestamp_1",
        "c_timestamp_3",
        "c_timestamp_6",
        "c_time",
        "c_time_1",
        "c_time_3",
        "c_time_6",
        "c_year",
        "c_year_4",
        "c_char",
        "c_varchar",
        "c_binary",
        "c_varbinary",
        "c_blob_tiny",
        "c_blob",
        "c_blob_medium",
        "c_blob_long",
        "c_text_tiny",
        "c_text",
        "c_text_medium",
        "c_text_long"
    );

    protected static String EXPIRE = "alter table %s expire local partition %s";

    protected Set<String> reservedColumns = ImmutableSet.<String>builder().add("id").add("gmt_modified").build();

    public FileStorageColumnDDLBaseTest(String crossSchema, String auto, String seed) {
        this.crossSchema = Boolean.parseBoolean(crossSchema);
        this.auto = Boolean.parseBoolean(auto);
        this.r1 = new Random(Long.parseLong(seed));
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    protected final String fullTypeTable = "baseTable";

    protected String compareTable = "compare";

    protected String innodbTable = "ttlInno";

    protected String ossTable = "ttlOSS";

    protected Connection getFullTypeConn() {
        return getTddlConnection1();
    }

    protected String getFullTypeSchema() {
        return tddlDatabase1;
    }

    protected Connection getCompareConn() {
        return getTddlConnection1();
    }

    protected String getCompareSchema() {
        return tddlDatabase1;
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

    protected String getOssSchema() {
        return crossSchema ? tddlDatabase2 : tddlDatabase1;
    }

    protected Random r1;

    @Before
    public void prepareTable() {
        try {
            // drop tables
            dropTableIfExists(getFullTypeConn(), fullTypeTable);
            dropTableIfExists(getCompareConn(), compareTable);
            dropTableIfExists(getOssConn(), ossTable);
            dropTableIfExists(getInnoConn(), innodbTable);

            dateList = Lists.newArrayList();

            // build tables
            FullTypeTestUtil.createInnodbTableWithData(getFullTypeConn(), fullTypeTable);
            LocalDate now = LocalDate.now();
            LocalDate startWithDate = now.minusMonths(12L);
            JdbcUtil.executeSuccess(getCompareConn(),
                String.format(auto ? TABLE_DEFINITION_FORMAT_AUTO : TABLE_DEFINITION_FORMAT, compareTable,
                    startWithDate.plusMonths(1L)));
            JdbcUtil.executeSuccess(getInnoConn(),
                String.format(auto ? TABLE_DEFINITION_FORMAT_AUTO : TABLE_DEFINITION_FORMAT, innodbTable,
                    startWithDate.plusMonths(1L)));
            JdbcUtil.executeSuccess(getOssConn(),
                String.format("create table %s like %s.%s engine = '%s' archive_mode = 'ttl';", ossTable,
                    getInnoSchema(), innodbTable, engine.name()));

            // get all date
            while (startWithDate.isBefore(now)) {
                dateList.add(startWithDate);
                startWithDate = startWithDate.plusMonths(1L);
            }

            // get all local partition
            localPartitionQueue = Queues.newArrayDeque();
            ResultSet rs = JdbcUtil.executeQuery(String.format(
                "select LOCAL_PARTITION_NAME from information_schema.local_partitions where table_schema=\"%s\" and table_name=\"%s\"",
                getInnoSchema(), innodbTable), getInnoConn());
            while (rs.next()) {
                localPartitionQueue.offer(rs.getString("LOCAL_PARTITION_NAME"));
            }
            rs.close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @After
    public void afterDDLBaseNewDBTestCase() {
        //cleanDataBase();
    }

    protected void performDdl(String ddl) {
        if (StringUtil.isEmpty(ddl)) {
            return;
        }
        if (ddl.endsWith(",")) {
            ddl = ddl.substring(0, ddl.length() - 1);
        }
        JdbcUtil.executeSuccess(getCompareConn(), String.format(ddl, compareTable));
        JdbcUtil.executeSuccess(getInnoConn(), String.format(ddl, innodbTable));
    }

    protected Set<String> getColumns() {
        Set<String> columns = Sets.newTreeSet();
        String SHOW_COLUMNS = "show columns from %s";
        try (ResultSet rs = JdbcUtil.executeQuery(String.format(SHOW_COLUMNS, innodbTable), getInnoConn())) {
            while (rs.next()) {
                String col = rs.getString("Field");
                if (!reservedColumns.contains(col)) {
                    columns.add(col);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return columns;
    }

    protected String buildNewColumn(String target, Set<String> columns) {
        if (columns.add(target)) {
            return target;
        }
        String result = target + RandomUtils.getStringWithCharacter(4);
        while (!columns.add(result)) {
            result = target + RandomUtils.getStringWithCharacter(4);
        }
        return result;
    }

    protected void buildBasicColumn() {
        // add random columns first
        String insertSql = "INSERT INTO %s(%s gmt_modified) select %s '%s' from %s.%s";

        StringBuilder insertColumns = new StringBuilder();
        StringBuilder selectColumns = new StringBuilder();
        Set<String> columns = getColumns();

        StringBuilder ddl = new StringBuilder("alter table %s add column (");
        for (int i = 0; i < allColumns.size(); i++) {
            String column = allColumns.get(i);
            String newColumn = buildNewColumn(column, columns);
            String columnDef =
                FullTypeSeparatedTestUtil.allColumnDefinitions.get(column).replace(column, newColumn)
                    .replace(",\n", "");
            ddl.append(i > 0 ? "," : "").append(columnDef);
            insertColumns.append(newColumn).append(",");
            selectColumns.append(column).append(",");
        }
        ddl.append(")");

        // perform ddl
        performDdl(ddl.toString());

        // insert
        for (LocalDate date : dateList) {
            JdbcUtil.executeSuccess(getCompareConn(),
                String.format(insertSql, compareTable, insertColumns, selectColumns, date, getFullTypeSchema(),
                    fullTypeTable));
            JdbcUtil.executeSuccess(getInnoConn(),
                String.format(insertSql, innodbTable, insertColumns, selectColumns, date, getFullTypeSchema(),
                    fullTypeTable));
        }

        // expire local partition
        JdbcUtil.executeSuccess(getInnoConn(), String.format(EXPIRE, innodbTable, localPartitionQueue.poll()));

        // check correctness
        checkAgg();
    }

    protected void realTest() {
        // perform ddl
        // insert
        // expire local partition
        // check correctness
    }

    @Test
    public void testDDL() {
        buildBasicColumn();
        realTest();
    }

    protected void checkAgg() {
        String compareSql;
        String ttlSql;

        // check agg column by column
        for (String singleColumn : getColumns()) {
            compareSql = String.format("select check_sum(%s) from %s", singleColumn, compareTable);
            ttlSql = String.format("select check_sum(%s) from (select %s from %s union all select %s from %s.%s)",
                singleColumn, singleColumn, innodbTable, singleColumn, getOssSchema(), ossTable);
            DataValidator.selectContentSameAssertWithDiffSql(ttlSql, compareSql, null,
                getInnoConn(), getCompareConn(), true, false, true);
        }

        String columns = String.join(",", getColumns());
        compareSql = String.format("select check_sum(%s) from %s", columns, compareTable);

        ttlSql = String.format("select check_sum(%s) from (select %s from %s union all select %s from %s.%s)",
            columns, columns, innodbTable, columns, getOssSchema(), ossTable);
        DataValidator.selectContentSameAssertWithDiffSql(ttlSql, compareSql, null,
            getInnoConn(), getCompareConn(), true, false, true);

        compareSql = String.format("select count(*) from %s", compareTable);

        ttlSql =
            String.format("select sum(x) from (select count(*) as x from %s union all select count(*) as x from %s.%s)",
                innodbTable, getOssSchema(), ossTable);

        DataValidator.selectContentSameAssertWithDiffSql(ttlSql, compareSql, null,
            getInnoConn(), getCompareConn(), true, false, true);
    }

    protected void checkFilter() {
        String selectSql = "select %s from %s where %s is not null limit 1";

        Set<String> columnSet = getColumns();
        String columns = String.join(",", getColumns());

        String compareSql;
        String ttlSql;

        for (String column : columnSet) {
            ResultSet rs =
                JdbcUtil.executeQuery(String.format(selectSql, column, compareTable, column), getCompareConn());
            Object goal;
            try {
                rs.next();
                goal = rs.getObject(1);
            } catch (SQLException ignore) {
                continue;
            }

            // ignore big_integer,float,double,bit

            if (goal == null || goal instanceof BigInteger || goal instanceof byte[]) {
                continue;
            }
            List<Object> para = ImmutableList.of(goal);

            boolean checkLike = true;
            checkLike = !(goal instanceof Boolean);
            if (checkLike) {
                compareSql =
                    String.format("select check_sum(%s) from %s where %s like ?", columns, compareTable, column);
                ttlSql = String.format(
                    "select check_sum(%s) from (select %s from %s  union all select %s from %s.%s) where %s like ?",
                    columns, columns, innodbTable, columns, getOssSchema(), ossTable, column);
                DataValidator.selectContentSameAssertWithDiffSql(ttlSql, compareSql, para,
                    getInnoConn(), getCompareConn(), true, false, true);

            }
            // ignore big_integer,float,double,bit
            if (goal instanceof Float ||
                goal instanceof Double) {
                continue;
            }
            compareSql =
                String.format("select check_sum(%s) from %s where %s = ?", columns, compareTable, column);
            ttlSql = String.format(
                "select check_sum(%s) from (select %s from %s  union all select %s from %s.%s) where %s = ?",
                columns, columns, innodbTable, columns, getOssSchema(), ossTable, column);
            DataValidator.selectContentSameAssertWithDiffSql(ttlSql, compareSql, para,
                getInnoConn(), getCompareConn(), true, false, true);
        }
    }
}
