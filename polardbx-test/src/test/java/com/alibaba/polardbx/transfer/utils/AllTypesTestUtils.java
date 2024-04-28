package com.alibaba.polardbx.transfer.utils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.security.SecureRandom;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class AllTypesTestUtils {

    /**
     * Big columns not included.
     * column name -> predefined values of this column
     */
    private final static Map<String, List<String>> VALUES_PROVIDER;
    /**
     * Big columns values providers.
     */
    private final static Map<String, List<String>> BIG_COLUMNS_VALUES_PROVIDER;
    private final static String TABLE_NAME = "`full_type`";

    public static Collection<String> getColumns() {
        return VALUES_PROVIDER.keySet();
    }

    public static Collection<String> getBigColumns() {
        return BIG_COLUMNS_VALUES_PROVIDER.keySet();
    }

    public static String buildInsertSql(int row, Collection<String> columns) {
        Collection<Collection<String>> values = new ArrayList<>(row);

        while (row-- > 0) {
            Collection<String> oneRow = new ArrayList<>();
            for (String column : columns) {
                oneRow.add(getRandomValue(column));
            }
            values.add(oneRow);
        }
        return buildInsertSql(columns, values);
    }

    public static String buildAllInsertSql() {
        List<String> columns = new ArrayList<>(VALUES_PROVIDER.keySet());
        columns.addAll(BIG_COLUMNS_VALUES_PROVIDER.keySet());

        int maxRows = 0;
        for (String column : columns) {
            Collection<String> values;
            if (null != (values = BIG_COLUMNS_VALUES_PROVIDER.get(column))) {
                maxRows = Math.max(maxRows, values.size() - 1);
            } else {
                values = VALUES_PROVIDER.get(column);
                maxRows = Math.max(maxRows, values.size());
            }
        }

        Collection<Collection<String>> values = new ArrayList<>();
        for (int i = 0; i < maxRows; i++) {
            Collection<String> oneRow = new ArrayList<>();
            for (String column : columns) {
                int index = i;
                List<String> possibleValues = BIG_COLUMNS_VALUES_PROVIDER.get(column);
                if (null == possibleValues) {
                    possibleValues = VALUES_PROVIDER.get(column);
                    if (index >= possibleValues.size()) {
                        index = possibleValues.size() - 1;
                    }
                } else {
                    // Not generate random value for big column, it may be too big.
                    if (index >= possibleValues.size() - 1) {
                        index = possibleValues.size() - 2;
                    }
                }
                oneRow.add(possibleValues.get(index));
            }
            values.add(oneRow);
        }

        return buildInsertSql(columns, values);
    }

    public static String buildUpdateSql(long id, Collection<String> columns) {
        Collection<String> values = new ArrayList<>();
        for (String column : columns) {
            values.add(getRandomValue(column));
        }
        return buildUpdateSql(id, columns, values);
    }

    public static String buildDeleteSql(long id) {
        return "DELETE FROM " + TABLE_NAME + " WHERE id = " + id;
    }

    public static String buildSelectRandomSql() throws SQLException {
        return "SELECT id FROM " + TABLE_NAME + " ORDER BY RAND() LIMIT 1";
    }

    /**
     * Build batch insert sql.
     *
     * @param columns column name
     * @param values outer list is the list of each row, inner list is the list of each value of one row
     * @return insert sql
     */
    private static String buildInsertSql(Collection<String> columns, Collection<Collection<String>> values) {
        StringBuilder sb = new StringBuilder("INSERT INTO ").append(TABLE_NAME);
        String columnList = String.join(",", columns);
        sb.append("(").append(columnList).append(") VALUES ");
        boolean first = true;
        for (Collection<String> value : values) {
            if (value.size() != columns.size()) {
                throw new IllegalArgumentException("Size of column list and the value list not matched.");
            }
            if (first) {
                first = false;
            } else {
                sb.append(",");
            }
            String row = String.join(",", value);
            sb.append("(").append(row).append(")");
        }
        return sb.toString();
    }

    /**
     * Randomly update one row.
     *
     * @param columns updated columns
     * @param values updated column values
     * @return update sql
     */
    private static String buildUpdateSql(long id, Collection<String> columns, Collection<String> values) {
        StringBuilder sb = new StringBuilder("UPDATE ").append(TABLE_NAME).append(" SET ");
        if (columns.size() != values.size()) {
            throw new IllegalArgumentException("Size of column list and the value list not matched.");
        }
        Iterator<String> iter0 = columns.iterator();
        Iterator<String> iter1 = values.iterator();
        boolean first = true;
        while (iter0.hasNext()) {
            if (first) {
                first = false;
            } else {
                sb.append(",");
            }
            sb.append(iter0.next()).append("=").append(iter1.next());
        }
        sb.append(" WHERE id = ").append(id);
        return sb.toString();
    }

    private static String getRandomValue(String column) {
        List<String> possibleValues = BIG_COLUMNS_VALUES_PROVIDER.get(column);
        if (null == possibleValues) {
            possibleValues = VALUES_PROVIDER.get(column);
        }
        if (null == possibleValues) {
            throw new IllegalArgumentException("Invalid column name");
        }
        return possibleValues.get(new SecureRandom().nextInt(possibleValues.size()));
    }

    public static final String FULL_TYPE_TABLE_TEMPLATE = "CREATE TABLE IF NOT EXISTS " + TABLE_NAME + " (\n"
        + "  `id` bigint(20) NOT NULL AUTO_INCREMENT,\n"
        + "  `c_bit_1` bit(1) DEFAULT NULL,\n"
        + "  `c_bit_8` bit(8) DEFAULT NULL,\n"
        + "  `c_bit_16` bit(16) DEFAULT NULL,\n"
        + "  `c_bit_32` bit(32) DEFAULT NULL,\n"
        + "  `c_bit_64` bit(64) DEFAULT NULL,\n"
        + "  `c_tinyint_1` tinyint(1) DEFAULT NULL,\n"
        + "  `c_tinyint_1_un` tinyint(1) unsigned DEFAULT NULL,\n"
        + "  `c_tinyint_4` tinyint(4) DEFAULT NULL,\n"
        + "  `c_tinyint_4_un` tinyint(4) unsigned DEFAULT NULL,\n"
        + "  `c_tinyint_8` tinyint(8) DEFAULT NULL,\n"
        + "  `c_tinyint_8_un` tinyint(8) unsigned DEFAULT NULL,\n"
        + "  `c_smallint_1` smallint(1) DEFAULT NULL,\n"
        + "  `c_smallint_16` smallint(16) DEFAULT NULL,\n"
        + "  `c_smallint_16_un` smallint(16) unsigned DEFAULT NULL,\n"
        + "  `c_mediumint_1` mediumint(1) DEFAULT NULL,\n"
        + "  `c_mediumint_24` mediumint(24) DEFAULT NULL,\n"
        + "  `c_mediumint_24_un` mediumint(24) unsigned DEFAULT NULL,\n"
        + "  `c_int_1` int(1) DEFAULT NULL,\n"
        + "  `c_int_32` int(32) NOT NULL DEFAULT 0 COMMENT \"For multi pk.\",\n"
        + "  `c_int_32_un` int(32) unsigned DEFAULT NULL,\n"
        + "  `c_bigint_1` bigint(1) DEFAULT NULL,\n"
        + "  `c_bigint_64` bigint(64) DEFAULT NULL,\n"
        + "  `c_bigint_64_un` bigint(64) unsigned DEFAULT NULL,\n"
        + "  `c_decimal` decimal DEFAULT NULL,\n"
        + "  `c_decimal_pr` decimal(3,3) DEFAULT NULL,\n"
        + "  `c_float` float DEFAULT NULL,\n"
        + "  `c_float_pr` float(10,3) DEFAULT NULL,\n"
        + "  `c_float_un` float(10,3) unsigned DEFAULT NULL,\n"
        + "  `c_double` double DEFAULT NULL,\n"
        + "  `c_double_pr` double(10,3) DEFAULT NULL,\n"
        + "  `c_double_un` double(10,3) unsigned DEFAULT NULL,\n"
        + "  `c_date` date DEFAULT NULL COMMENT \"date\",\n"
        + "  `c_datetime` datetime DEFAULT NULL,\n"
        + "  `c_datetime_1` datetime(1) DEFAULT NULL,\n"
        + "  `c_datetime_3` datetime(3) DEFAULT NULL,\n"
        + "  `c_datetime_6` datetime(6) DEFAULT NULL,\n"
        + "  `c_timestamp` timestamp NULL DEFAULT NULL,\n"
        + "  `c_timestamp_1` timestamp(1) NULL DEFAULT NULL,\n"
        + "  `c_timestamp_3` timestamp(3) NULL DEFAULT NULL,\n"
        + "  `c_timestamp_6` timestamp(6) NULL DEFAULT NULL,\n"
        + "  `c_time` time DEFAULT NULL,\n"
        + "  `c_time_1` time(1) DEFAULT NULL,\n"
        + "  `c_time_3` time(3) DEFAULT NULL,\n"
        + "  `c_time_6` time(6) DEFAULT NULL,\n"
        + "  `c_year` year DEFAULT NULL,\n"
        + "  `c_year_4` year(4) DEFAULT NULL,\n"
        + "  `c_char` char(10) DEFAULT NULL,\n"
        + "  `c_varchar` varchar(10) DEFAULT NULL,\n"
        + "  `c_binary` binary(10) DEFAULT NULL,\n"
        + "  `c_varbinary` varbinary(10) DEFAULT NULL,\n"
        + "  `c_blob_tiny` tinyblob DEFAULT NULL,\n"
        + "  `c_blob` blob DEFAULT NULL,\n"
        + "  `c_blob_medium` mediumblob DEFAULT NULL,\n"
        + "  `c_blob_long` longblob DEFAULT NULL,\n"
        + "  `c_text_tiny` tinytext DEFAULT NULL,\n"
        + "  `c_text` text DEFAULT NULL,\n"
        + "  `c_text_medium` mediumtext DEFAULT NULL,\n"
        + "  `c_text_long` longtext DEFAULT NULL,\n"
        + "  `c_enum` enum(\"a\",\"b\",\"c\") DEFAULT NULL,\n"
        + "  `c_set` set(\"a\",\"b\",\"c\") DEFAULT NULL,\n"
        + "  `c_json` json DEFAULT NULL,\n"
        + "  `c_geometry` geometry DEFAULT NULL,\n"
        + "  `c_point` point DEFAULT NULL,\n"
        + "  `c_linestring` linestring DEFAULT NULL,\n"
        + "  `c_polygon` polygon DEFAULT NULL,\n"
        + "  `c_multipoint` multipoint DEFAULT NULL,\n"
        + "  `c_multilinestring` multilinestring DEFAULT NULL,\n"
        + "  `c_multipolygon` multipolygon DEFAULT NULL,\n"
        + "  `c_geometrycollection` geometrycollection DEFAULT NULL,\n"
        + "  PRIMARY KEY (`id`)\n"
        + ") ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 ";

    public static final String C_ID = "id";
    public static final String C_BIT_1 = "c_bit_1";
    public static final String C_BIT_8 = "c_bit_8";
    public static final String C_BIT_16 = "c_bit_16";
    public static final String C_BIT_32 = "c_bit_32";
    public static final String C_BIT_64 = "c_bit_64";
    public static final String C_TINYINT_1 = "c_tinyint_1";
    public static final String C_TINYINT_1_UN = "c_tinyint_1_un";
    public static final String C_TINYINT_4 = "c_tinyint_4";
    public static final String C_TINYINT_4_UN = "c_tinyint_4_un";
    public static final String C_TINYINT_8 = "c_tinyint_8";
    public static final String C_TINYINT_8_UN = "c_tinyint_8_un";
    public static final String C_SMALLINT_1 = "c_smallint_1";
    public static final String C_SMALLINT_16 = "c_smallint_16";
    public static final String C_SMALLINT_16_UN = "c_smallint_16_un";
    public static final String C_MEDIUMINT_1 = "c_mediumint_1";
    public static final String C_MEDIUMINT_24 = "c_mediumint_24";
    public static final String C_MEDIUMINT_24_UN = "c_mediumint_24_un";
    public static final String C_INT_1 = "c_int_1";
    public static final String C_INT_32 = "c_int_32";
    public static final String C_INT_32_UN = "c_int_32_un";
    public static final String C_BIGINT_1 = "c_bigint_1";
    public static final String C_BIGINT_64 = "c_bigint_64";
    public static final String C_BIGINT_64_UN = "c_bigint_64_un";
    public static final String C_DECIMAL = "c_decimal";
    public static final String C_DECIMAL_PR = "c_decimal_pr";
    public static final String C_FLOAT = "c_float";
    public static final String C_FLOAT_PR = "c_float_pr";
    public static final String C_FLOAT_UN = "c_float_un";
    public static final String C_DOUBLE = "c_double";
    public static final String C_DOUBLE_PR = "c_double_pr";
    public static final String C_DOUBLE_UN = "c_double_un";
    public static final String C_DATE = "c_date";
    public static final String C_DATETIME = "c_datetime";
    public static final String C_DATETIME_1 = "c_datetime_1";
    public static final String C_DATETIME_3 = "c_datetime_3";
    public static final String C_DATETIME_6 = "c_datetime_6";
    public static final String C_TIMESTAMP = "c_timestamp";
    public static final String C_TIMESTAMP_1 = "c_timestamp_1";
    public static final String C_TIMESTAMP_3 = "c_timestamp_3";
    public static final String C_TIMESTAMP_6 = "c_timestamp_6";
    public static final String C_TIME = "c_time";
    public static final String C_TIME_1 = "c_time_1";
    public static final String C_TIME_3 = "c_time_3";
    public static final String C_TIME_6 = "c_time_6";
    public static final String C_YEAR = "c_year";
    public static final String C_YEAR_4 = "c_year_4";
    public static final String C_CHAR = "c_char";
    public static final String C_VARCHAR = "c_varchar";
    public static final String C_BINARY = "c_binary";
    public static final String C_VARBINARY = "c_varbinary";
    public static final String C_BLOB_TINY = "c_blob_tiny";
    public static final String C_BLOB = "c_blob";
    public static final String C_BLOB_MEDIUM = "c_blob_medium";
    public static final String C_BLOB_LONG = "c_blob_long";
    public static final String C_TEXT_TINY = "c_text_tiny";
    public static final String C_TEXT = "c_text";
    public static final String C_TEXT_MEDIUM = "c_text_medium";
    public static final String C_TEXT_LONG = "c_text_long";
    public static final String C_ENUM = "c_enum";
    public static final String C_SET = "c_set";
    public static final String C_JSON = "c_json";
    public static final String C_GEOMETRY = "c_geometry";
    public static final String C_POINT = "c_point";
    public static final String C_LINESTRING = "c_linestring";
    public static final String C_POLYGON = "c_polygon";
    public static final String C_MULTIPOINT = "c_multipoint";
    public static final String C_MULTILINESTRING = "c_multilinestring";
    public static final String C_MULTIPOLYGON = "c_multipolygon";
    public static final String C_GEOMETRYCOLLECTION = "c_geometrycollection";

    static {
        Map<String, List<String>> all = buildFullTypeTestValues();
        final ImmutableMap.Builder<String, List<String>> builder0 = ImmutableMap.builder();
        final ImmutableMap.Builder<String, List<String>> builder1 = ImmutableMap.builder();
        for (Map.Entry<String, List<String>> obj : all.entrySet()) {
            switch (obj.getKey()) {
            case C_BLOB:
            case C_BLOB_MEDIUM:
            case C_BLOB_LONG:
            case C_TEXT:
            case C_TEXT_MEDIUM:
            case C_TEXT_LONG:
                builder1.put(obj.getKey(), obj.getValue());
                break;
            default:
                builder0.put(obj.getKey(), obj.getValue());
            }
        }
        VALUES_PROVIDER = builder0.build();
        BIG_COLUMNS_VALUES_PROVIDER = builder1.build();
    }

    /**
     * Corner cases included.
     *
     * @return <column name, list of values>
     */
    private static ImmutableMap<String, List<String>> buildFullTypeTestValues() {
        final ImmutableMap.Builder<String, List<String>> builder = ImmutableMap.builder();

        builder.put(C_BIT_1, ImmutableList.of(
            "0",
            "1",
            "2",
            "FLOOR(RAND() + 0.5)"
        ));

        builder.put(C_BIT_8, ImmutableList.of(
            "0",
            "1",
            "2",
            "256",
            "CAST((RAND() * 255) AS UNSIGNED)"
        ));

        builder.put(C_BIT_16, ImmutableList.of(
            "0",
            "1",
            "2",
            "65535",
            "CAST((RAND() * 65535) AS UNSIGNED)"
        ));

        builder.put(C_BIT_32, ImmutableList.of(
            "0",
            "1",
            "2",
            "4294967296",
            "CAST((RAND() * 4294967295) AS UNSIGNED)"
        ));

        builder.put(C_BIT_64, ImmutableList.of(
            "0",
            "1",
            "2",
            "18446744073709551615",
            "(CAST(RAND() * 4294967295 AS UNSIGNED) << 32) | CAST(RAND() * 4294967295 AS UNSIGNED)"
        ));

        builder.put(C_TINYINT_1, ImmutableList.of(
            "-1",
            "0",
            "1",
            "127",
            "CAST((RAND() * 255) - 128 AS SIGNED)"
        ));

        builder.put(C_TINYINT_1_UN, ImmutableList.of(
            "-1",
            "0",
            "1",
            "127",
            "255",
            "CAST(RAND() * 255 AS UNSIGNED)"
        ));

        builder.put(C_TINYINT_4, ImmutableList.of(
            "-1",
            "0",
            "1",
            "127",
            "CAST(RAND() * 255 - 128 AS SIGNED)"
        ));

        builder.put(C_TINYINT_4_UN, ImmutableList.of(
            "-1",
            "0",
            "1",
            "127",
            "255",
            "CAST(RAND() * 255 AS UNSIGNED)"
        ));

        builder.put(C_TINYINT_8, ImmutableList.of(
            "-1",
            "0",
            "1",
            "127",
            "CAST(RAND() * 255 - 128 AS SIGNED)"
        ));

        builder.put(C_TINYINT_8_UN, ImmutableList.of(
            "-1",
            "0",
            "1",
            "127",
            "255",
            "CAST(RAND() * 255 AS UNSIGNED)"
        ));

        builder.put(C_SMALLINT_1, ImmutableList.of(
            "-1",
            "0",
            "1",
            "65535",
            "CAST(RAND() * 65535 AS SIGNED) - 32768"
        ));

        builder.put(C_SMALLINT_16, ImmutableList.of(
            "-1",
            "0",
            "1",
            "65535",
            "CAST(RAND() * 65535 AS SIGNED) - 32768"
        ));

        builder.put(C_SMALLINT_16_UN, ImmutableList.of(
            "-1",
            "0",
            "1",
            "65535",
            "CAST(RAND() * 65535 AS UNSIGNED)"
        ));

        builder.put(C_MEDIUMINT_1, ImmutableList.of(
            "-1",
            "0",
            "1",
            "16777215",
            "CAST(RAND() * 16777215 AS SIGNED) - 8388608"
        ));

        builder.put(C_MEDIUMINT_24, ImmutableList.of(
            "-1",
            "0",
            "1",
            "16777215",
            "CAST(RAND() * 16777215 AS SIGNED) - 8388608"
        ));

        builder.put(C_MEDIUMINT_24_UN, ImmutableList.of(
            "-1",
            "0",
            "1",
            "16777215",
            "CAST(RAND() * 16777215 AS UNSIGNED)"
        ));

        builder.put(C_INT_1, ImmutableList.of(
            "-1",
            "0",
            "1",
            /*"4294967295",*/
            "CAST(RAND() * 4294967295 AS SIGNED) - 2147483648"
        ));

        builder.put(C_INT_32, ImmutableList.of(
            "-1",
            "0",
            "1",
            "4294967295",
            "CAST(RAND() * 4294967295 AS SIGNED) - 2147483648"
        ));

        builder.put(C_INT_32_UN, ImmutableList.of(
            "-1",
            "0",
            "1",
            "4294967295",
            "CAST(RAND() * 4294967295 AS UNSIGNED)"
        ));

        builder.put(C_BIGINT_1, ImmutableList.of(
            "-1",
            "0",
            "1",
            "18446744073709551615",
            "CAST((RAND() - 0.5) * 18446744073709551615 AS SIGNED)"
        ));

        builder.put(C_BIGINT_64, ImmutableList.of(
            "-1",
            "0",
            "1",
            "18446744073709551615",
            "CAST((RAND() - 0.5) * 18446744073709551615 AS SIGNED)"
        ));

        builder.put(C_BIGINT_64_UN, ImmutableList.of(
            "-1",
            "0",
            "1",
            "18446744073709551615",
            "(CAST(RAND() * 4294967295 AS UNSIGNED) << 32) | CAST(RAND() * 4294967295 AS UNSIGNED)"
        ));

        builder.put(C_DECIMAL, ImmutableList.of(
            "'100.000'",
            "'100.003'",
            "'-100.003'",
            "'-100.0000001'",
            "CAST(RAND() * 100000 AS DECIMAL(10,0))"
        ));

        builder.put(C_DECIMAL_PR, ImmutableList.of(
            "'100.000'",
            "'100.003'",
            "'-100.003'",
            "'-100.0000001'",
            "'5.576856765031534000000000000000'",
            "ROUND(RAND(), 3)"
        ));

        builder.put(C_FLOAT, ImmutableList.of(
            "'100.000'",
            "'100.003'",
            "'-100.003'",
            "'-100.0000001'",
            "(RAND() * -10000)",
            "(RAND() * 10000)"
        ));

        builder.put(C_FLOAT_PR, ImmutableList.of(
            "'100.000'",
            "'100.003'",
            "'-100.003'",
            "'-100.0000001'",
            "(RAND() * -10000)",
            "(RAND() * 10000)"
        ));

        builder.put(C_FLOAT_UN, ImmutableList.of(
            "'100.000'",
            "'100.003'",
            "'-100.003'",
            "'-100.0000001'",
            "(RAND() * 10000)"
        ));

        builder.put(C_DOUBLE, ImmutableList.of(
            "'100.000'",
            "'100.003'",
            "'-100.003'",
            "'-100.0000001'",
            "(RAND() * -10000)",
            "(RAND() * 10000)"
        ));

        builder.put(C_DOUBLE_PR, ImmutableList.of(
            "'100.000'",
            "'100.003'",
            "'-100.003'",
            "'-100.0000001'",
            "(RAND() * -10000)",
            "(RAND() * 10000)"
        ));

        builder.put(C_DOUBLE_UN, ImmutableList.of(
            "'100.000'",
            "'100.003'",
            "'-100.003'",
            "'-100.0000001'",
            "(RAND() * 10000)"
        ));

        builder.put(C_DATE, ImmutableList.of(
            "'0000-00-00'",
            "'9999-12-31'",
            "'0000-00-00 01:01:01'",
            "'1969-09-00'",
            "'2018-00-00'",
            "'2017-12-12'",
            "DATE_ADD('1970-01-01', INTERVAL FLOOR(RAND() * (DATEDIFF('2024-12-31', '1970-01-01') + 1)) DAY)"
        ));

        builder.put(C_DATETIME, ImmutableList.of(
            "'0000-00-00 00:00:00'",
            "'9999-12-31 23:59:59'",
            "'0000-00-00 01:01:01'",
            "'1969-09-00 23:59:59'",
            "'2018-00-00 00:00:00'",
            "'2017-12-12 23:59:59'",
            "FROM_UNIXTIME(RAND() * (UNIX_TIMESTAMP('2024-12-31 23:59:59') - UNIX_TIMESTAMP('1970-01-01 00:00:00')) + UNIX_TIMESTAMP('1970-01-01 00:00:00'), '%Y-%m-%d %H:%i:%s.%f')"
        ));

        builder.put(C_DATETIME_1, ImmutableList.of(
            "'0000-00-00 00:00:00.0'",
            "'9999-12-31 23:59:59.9'",
            "'0000-00-00 01:01:01.12'",
            "'1969-09-00 23:59:59.06'",
            "'2018-00-00 00:00:00.04'",
            "'2017-12-12 23:59:59.045'",
            "FROM_UNIXTIME(RAND() * (UNIX_TIMESTAMP('2024-12-31 23:59:59') - UNIX_TIMESTAMP('1970-01-01 00:00:00')) + UNIX_TIMESTAMP('1970-01-01 00:00:00'), '%Y-%m-%d %H:%i:%s.%f')"
        ));

        builder.put(C_DATETIME_3, ImmutableList.of(
            "'0000-00-00 00:00:00.000'",
            "'9999-12-31 23:59:59.999'",
            "'0000-00-00 01:01:01.121'",
            "'1969-09-00 23:59:59.0006'",
            "'2018-00-00 00:00:00.0004'",
            "'2017-12-12 23:59:59.00045'",
            "FROM_UNIXTIME(RAND() * (UNIX_TIMESTAMP('2024-12-31 23:59:59') - UNIX_TIMESTAMP('1970-01-01 00:00:00')) + UNIX_TIMESTAMP('1970-01-01 00:00:00'), '%Y-%m-%d %H:%i:%s.%f')"
        ));

        builder.put(C_DATETIME_6, ImmutableList.of(
            "'0000-00-00 00:00:00.000000'",
            "'9999-12-31 23:59:59.999999'",
            "'0000-00-00 01:01:01.121121'",
            "'1969-09-00 23:59:59.0000006'",
            "'2018-00-00 00:00:00.0000004'",
            "'2017-12-12 23:59:59.00000045'",
            "FROM_UNIXTIME(RAND() * (UNIX_TIMESTAMP('2024-12-31 23:59:59') - UNIX_TIMESTAMP('1970-01-01 00:00:00')) + UNIX_TIMESTAMP('1970-01-01 00:00:00'), '%Y-%m-%d %H:%i:%s.%f')"
        ));

        builder.put(C_TIMESTAMP, ImmutableList.of(
            "'0000-00-00 00:00:00'",
            "'9999-12-31 23:59:59'",
            "'0000-00-00 01:01:01'",
            "'1969-09-00 23:59:59'",
            "'2018-00-00 00:00:00'",
            "'2017-12-12 23:59:59'",
            "'2038-01-19 03:14:07'",
            "'2039-01-19 03:14:07'",
            "FROM_UNIXTIME(RAND() * (UNIX_TIMESTAMP('2024-12-31 23:59:59') - UNIX_TIMESTAMP('1970-01-01 00:00:00')) + UNIX_TIMESTAMP('1970-01-01 00:00:00'), '%Y-%m-%d %H:%i:%s.%f')"
        ));

        builder.put(C_TIMESTAMP_1, ImmutableList.of(
            "'0000-00-00 00:00:00.0'",
            "'9999-12-31 23:59:59.9'",
            "'0000-00-00 01:01:01.12'",
            "'1969-09-00 23:59:59.06'",
            "'2018-00-00 00:00:00.04'",
            "'2017-12-12 23:59:59.045'",
            "'2038-01-19 03:14:07.12'",
            "'2039-01-19 03:14:07.11'",
            "FROM_UNIXTIME(RAND() * (UNIX_TIMESTAMP('2024-12-31 23:59:59') - UNIX_TIMESTAMP('1970-01-01 00:00:00')) + UNIX_TIMESTAMP('1970-01-01 00:00:00'), '%Y-%m-%d %H:%i:%s.%f')"
        ));

        builder.put(C_TIMESTAMP_3, ImmutableList.of(
            "'0000-00-00 00:00:00.000'",
            "'9999-12-31 23:59:59.999'",
            "'0000-00-00 01:01:01.121'",
            "'1969-09-00 23:59:59.0006'",
            "'2018-00-00 00:00:00.0004'",
            "'2017-12-12 23:59:59.00045'",
            "'2038-01-19 03:14:07.011'",
            "'2039-01-19 03:14:07.618'",
            "FROM_UNIXTIME(RAND() * (UNIX_TIMESTAMP('2024-12-31 23:59:59') - UNIX_TIMESTAMP('1970-01-01 00:00:00')) + UNIX_TIMESTAMP('1970-01-01 00:00:00'), '%Y-%m-%d %H:%i:%s.%f')"
        ));

        builder.put(C_TIMESTAMP_6, ImmutableList.of(
            "'0000-00-00 00:00:00.000000'",
            "'9999-12-31 23:59:59.999999'",
            "'0000-00-00 01:01:01.121121'",
            "'1969-09-00 23:59:59.0000006'",
            "'2018-00-00 00:00:00.0000004'",
            "'2017-12-12 23:59:59.00000045'",
            "'2038-01-19 03:14:07.123456'",
            "'2039-01-19 03:14:07.789012'",
            "FROM_UNIXTIME(RAND() * (UNIX_TIMESTAMP('2024-12-31 23:59:59') - UNIX_TIMESTAMP('1970-01-01 00:00:00')) + UNIX_TIMESTAMP('1970-01-01 00:00:00'), '%Y-%m-%d %H:%i:%s.%f')"
        ));

        builder.put(C_TIME, ImmutableList.of(
            "'-838:59:59'",
            "'838:59:59'",
            "'00:00:00'",
            "'01:01:01'",
            "'-01:01:01'",
            "'23:59:59'",
            "SEC_TO_TIME(FLOOR(RAND() * 86400))"
        ));

        builder.put(C_TIME_1, ImmutableList.of(
            "'-838:59:59.9'",
            "'838:59:59.9'",
            "'00:00:00.1'",
            "'01:01:01.6'",
            "'-01:01:01.4'",
            "'23:59:59.45'",
            "CONCAT(SEC_TO_TIME(FLOOR(RAND() * 86400)), '.', LPAD(FLOOR(RAND() * 10), 1, '0'))"
        ));

        builder.put(C_TIME_3, ImmutableList.of(
            "'-838:59:59.999'",
            "'838:59:59.999'",
            "'00:00:00.111'",
            "'01:01:01.106'",
            "'-01:01:01.0004'",
            "'23:59:59.00045'",
            "CONCAT(SEC_TO_TIME(FLOOR(RAND() * 86400)), '.', LPAD(FLOOR(RAND() * 1000), 3, '0'))"
        ));

        builder.put(C_TIME_6, ImmutableList.of(
            "'-838:59:59.999999'",
            "'838:59:59.999999'",
            "'00:00:00.111111'",
            "'01:01:01.106106'",
            "'-01:01:01.0000004'",
            "'23:59:59.00000045'",
            "CONCAT(SEC_TO_TIME(FLOOR(RAND() * 86400)), '.', LPAD(FLOOR(RAND() * 1000000), 6, '0'))"
        ));

        builder.put(C_YEAR, ImmutableList.of(
            "'0000'",
            "'9999'",
            "'1970'",
            "'2000'",
            "'1969'",
            "'1901'",
            "'1900'",
            "'2155'",
            "'2156'",
            "FLOOR(1901 + RAND() * (2155 - 1901 + 1))"
        ));

        builder.put(C_YEAR_4, ImmutableList.of(
            "'0000'",
            "'9999'",
            "'1970'",
            "'2000'",
            "'1969'",
            "'1901'",
            "'1900'",
            "'2155'",
            "'2156'",
            "FLOOR(1901 + RAND() * (2155 - 1901 + 1))"
        ));

        builder.put(C_CHAR, ImmutableList.of(
            "'11'",
            "'99'",
            "'a中国a'",
            "x'313233616263'",
            "LPAD(CONV(FLOOR(RAND() * 9999999999), 10, 36), 10, '0')"
        ));

        builder.put(C_VARCHAR, ImmutableList.of(
            "'11'",
            "'99'",
            "'a中国a'",
            "x'313233616263'",
            "SUBSTRING(MD5(RAND()), 1, 10)"
        ));

        builder.put(C_BINARY, ImmutableList.of(
            "'11'",
            "'99'",
            "'a中国a'",
            "x'313233616263'",
            "x'0A08080E10011894AB0E'",
            "RANDOM_BYTES(10)"
        ));

        builder.put(C_VARBINARY, ImmutableList.of(
            "'11'",
            "'99'",
            "'a中国a'",
            "x'313233616263'",
            "x'0A08080E10011894AB0E'",
            "RANDOM_BYTES(FLOOR(1 + (RAND() * 10)))"
        ));

        builder.put(C_BLOB_TINY, ImmutableList.of(
            "'11'",
            "'99'",
            "'a中国a'",
            "x'313233616263'",
            "x'0A08080E10011894AB0E'",
            "RANDOM_BYTES(FLOOR(1 + (RAND() * 255)))"
        ));

        builder.put(C_BLOB, ImmutableList.of(
            "'11'",
            "'99'",
            "'a中国a'",
            "x'313233616263'",
            "x'0A08080E10011894AB0E'",
            /* from 64B to 64KB */
            "REPEAT(RANDOM_BYTES(FLOOR(1 + (RAND() * 1024))), 64)"
        ));

        builder.put(C_BLOB_MEDIUM, ImmutableList.of(
            "'11'",
            "'99'",
            "'a中国a'",
            "x'313233616263'",
            "x'0A08080E10011894AB0E'",
            /* from 16KB to 16MB */
            "REPEAT(RANDOM_BYTES(FLOOR(1 + (RAND() * 1024))), 16384)"
        ));

        builder.put(C_BLOB_LONG, ImmutableList.of(
            "'11'",
            "'99'",
            "'a中国a'",
            "x'313233616263'",
            "x'0A08080E10011894AB0E'",
            /* from 128KB to 128MB */
            "REPEAT(RANDOM_BYTES(FLOOR(1 + (RAND() * 1024))), 131072)"
        ));

        builder.put(C_TEXT_TINY, ImmutableList.of(
            "'11'",
            "'99'",
            "'a中国a'",
            "x'313233616263'",
            "SUBSTRING(MD5(RAND()), 1, 10)"
        ));

        builder.put(C_TEXT, ImmutableList.of(
            "'11'",
            "'99'",
            "'a中国a'",
            "x'313233616263'",
            "REPEAT(SUBSTRING(MD5(RAND()), 1, 10), 100)"
        ));

        builder.put(C_TEXT_MEDIUM, ImmutableList.of(
            "'11'",
            "'99'",
            "'a中国a'",
            "x'313233616263'",
            "REPEAT(SUBSTRING(MD5(RAND()), 1, 10), 10000)"
        ));

        builder.put(C_TEXT_LONG, ImmutableList.of(
            "'11'",
            "'99'",
            "'a中国a'",
            "x'313233616263'",
            "REPEAT(SUBSTRING(MD5(RAND()), 1, 10), 1000000)"
        ));

        builder.put(C_ENUM, ImmutableList.of(
            "'a'",
            "'b'",
//            "NULL",
            "ELT(FLOOR(1 + (RAND() * 3)), 'a', 'b', 'c')"
        ));

        builder.put(C_SET, ImmutableList.of(
            "'a'",
            "'b,a'",
            "'b,c,a'",
//            "'d'",
//            "'a,d'",
//            "NULL",
            "ELT(FLOOR(1 + (RAND() * 3)), 'a', 'b', 'c')"
        ));

        builder.put(C_JSON, ImmutableList.of(
//            "'{\"k1\": \"v1\", \"k2\": 10}'",
//            "'{\"k1\": \"v1\", \"k2\": [10, 20]}'",
            "NULL"
//            "JSON_OBJECT('k1', CHAR(97 + FLOOR(RAND() * 26)),'k2', CHAR(97 + FLOOR(RAND() * 26)))"
        ));

        builder.put(C_GEOMETRY, ImmutableList.of(
            "ST_PointFromText('POINT(15 20)')",
            "ST_PointFromText(CONCAT('POINT(', RAND() * 100, ' ', RAND() * 100, ')'))"
        ));

        builder.put(C_POINT, ImmutableList.of(
            "ST_PointFromText('POINT(15 20)')",
            "ST_PointFromText(CONCAT('POINT(', RAND() * 100, ' ', RAND() * 100, ')'))"
        ));

        builder.put(C_LINESTRING, ImmutableList.of(
            "ST_GeomFromText('LINESTRING(0 0, 10 10, 20 25, 50 60)')",
            "ST_LineStringFromText(CONCAT('LINESTRING(', RAND() * 360 - 180, ' ', RAND() * 180 - 90, ', ', RAND() * 360 - 180, ' ', RAND() * 180 - 90, ')'))"
        ));

        builder.put(C_POLYGON, ImmutableList.of(
            "ST_GeomFromText('POLYGON((0 0,10 0,10 10,0 10,0 0),(5 5,7 5,7 7,5 7, 5 5))')"
        ));

        builder.put(C_MULTIPOINT, ImmutableList.of(
            "ST_GeomFromText('MULTIPOINT(0 0, 15 25, 45 65)')",
            "ST_GeomFromText(CONCAT('MULTIPOINT(', RAND() * 360 - 180, ' ', RAND() * 180 - 90, ',', RAND() * 360 - 180, ' ', RAND() * 180 - 90, ',', RAND() * 360 - 180, ' ', RAND() * 180 - 90, ',', RAND() * 360 - 180, ' ', RAND() * 180 - 90, ')'))"
        ));

        builder.put(C_MULTILINESTRING, ImmutableList.of(
            "ST_GeomFromText('MULTILINESTRING((10 10, 20 20), (15 15, 30 15))')",
            "ST_GeomFromText(CONCAT('MULTILINESTRING((',RAND() * 360 - 180, ' ', RAND() * 180 - 90, ',', RAND() * 360 - 180, ' ', RAND() * 180 - 90, '),(', RAND() * 360 - 180, ' ', RAND() * 180 - 90, ',', RAND() * 360 - 180, ' ', RAND() * 180 - 90, '))'))"
        ));

        builder.put(C_MULTIPOLYGON, ImmutableList.of(
            "ST_GeomFromText('MULTIPOLYGON(((0 0,10 0,10 10,0 10,0 0)),((5 5,7 5,7 7,5 7, 5 5)))')"
        ));

        builder.put(C_GEOMETRYCOLLECTION, ImmutableList.of(
            "ST_GeomFromText('GEOMETRYCOLLECTION(POINT(10 10), POINT(30 30), LINESTRING(15 15, 20 20))')",
            "ST_GeomCollFromText(CONCAT('GEOMETRYCOLLECTION(','POINT(', RAND() * 360 - 180, ' ', RAND() * 180 - 90, '),', 'LINESTRING(', RAND() * 360 - 180, ' ', RAND() * 180 - 90, ',', RAND() * 360 - 180, ' ', RAND() * 180 - 90, '))'))"
        ));

        return builder.build();
    }

}
