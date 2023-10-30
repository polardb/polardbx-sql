/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.qatest.oss.ddl.index;

import com.alibaba.polardbx.qatest.oss.ddl.FileStorageColumnDDLBaseTest;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static com.google.common.truth.Truth.assertWithMessage;

public class FileStorageAddDropIndexTest extends FileStorageColumnDDLBaseTest {
    /**
     * | ADD {INDEX | KEY} [index_name]
     * [index_type] (key_part,...) [index_option] ...
     * | DROP {INDEX | KEY} index_name
     * | DROP PRIMARY KEY
     */
    public FileStorageAddDropIndexTest(String crossSchema, String seed) {
        super(crossSchema, "false", seed);
    }

    protected static List<String> IndexableColumns = ImmutableList.of(
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
        "c_varbinary"
    );

    protected Set<String> reservedIndexes = ImmutableSet.<String>builder().add("PRIMARY").build();

    private final static String SHOW_INDEXES = "show indexes from %s";

    private final static String SHOW_GLOBAL_INDEXES = "show global indexes from %s";

    private static String KeyNameOfGSI(String keyName) {
        return keyName.substring(0, keyName.length() - 6);
    }

    protected int indexCount = 0;

    @Parameterized.Parameters(name = "{index}:cross={0}")
    public static List<String[]> prepareData() {
        return Arrays.asList(
            new String[] {"false", "3"},
            new String[] {"true", "4"}
        );
    }

    protected Set<String> getIndexes(Connection conn, String table) {
        Set<String> indexes = Sets.newTreeSet();
        try (ResultSet rs = JdbcUtil.executeQuery(String.format(SHOW_INDEXES, table), conn)) {
            while (rs.next()) {
                String col = rs.getString("Key_name");
                if (!reservedIndexes.contains(col)) {
                    indexes.add(col);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return indexes;
    }

    protected Set<String> getIndexesWithoutGsi(Connection conn, String table) {
        Set<String> indexes = getIndexes(conn, table);
        Set<String> gsi = Sets.newTreeSet();
        try (ResultSet rs = JdbcUtil.executeQuery(String.format(SHOW_GLOBAL_INDEXES, table), conn)) {
            while (rs.next()) {
                String col = rs.getString("Key_name");
                gsi.add(KeyNameOfGSI(col));
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return Sets.difference(indexes, gsi);
    }

    protected void CheckIndexes() {
        Set<String> innoIndexes = getIndexes(getInnoConn(), innodbTable);
        Set<String> compIndexes = getIndexes(getCompareConn(), compareTable);
        assertWithMessage("表" + getInnoSchema() + "." + innodbTable +
            "与表" + getCompareSchema() + "." + compareTable + "索引不同").that(innoIndexes)
            .containsExactlyElementsIn(compIndexes);
        assertWithMessage("表" + getInnoSchema() + "." + innodbTable +
            "与表" + getOssSchema() + "." + ossTable + "局部索引不同").that(
                getIndexesWithoutGsi(getInnoConn(), innodbTable))
            .containsExactlyElementsIn(getIndexes(getOssConn(), ossTable));

    }

    protected Set<String> buildIndexColumns(String[] columnArray) {
        ImmutableSet.Builder<String> builder = ImmutableSet.builder();
        for (int i = 0; i < 2; i++) {
            String column = columnArray[r1.nextInt(columnArray.length)];
            if (FileStorageAddDropIndexTest.IndexableColumns.contains(column)) {
                builder.add(column);
            }
        }
        return builder.build();
    }

    protected void addIndex(Set<String> columns) {
        if (CollectionUtils.isEmpty(columns)) {
            return;
        }
        String ddl = "alter table %s add index index_" + (++indexCount) + "("
            + String.join(",", columns) + ")";
        performDdl(ddl);
    }

    protected String buildDDL1(String[] columnArray) {
        boolean success = false;
        Set<String> indexColumns = buildIndexColumns(columnArray);
        Set<String> indexes = getIndexes(getInnoConn(), innodbTable);
        StringBuilder ddl = new StringBuilder("alter table %s ");
        if (!CollectionUtils.isEmpty(indexColumns)) {
            success = true;
            ddl.append("add index index_").append(++indexCount)
                .append("(").append(String.join(",", indexColumns)).append("),");
        }

        if (!CollectionUtils.isEmpty(indexes)) {
            success = true;
            String droppedIndex = (String) indexes.toArray()[r1.nextInt(indexes.size())];
            ddl.append(" drop index ").append(droppedIndex);
        }
        return success ? ddl.toString() : null;
    }

    protected String buildDDL2(String[] columnArray) {
        return null;
    }

    @Override
    protected void realTest() {
        // add 10 random indexes first
        Set<String> columns = getColumns();
        String[] columnArray = columns.toArray(new String[0]);

        addIndex(ImmutableSet.of("c_varchar"));

        int addRound = 10;
        for (int round = 0; round < addRound; round++) {
            addIndex(buildIndexColumns(columnArray));
        }

        for (int round = 0; round < addRound; round++) {
            performDdl(buildDDL1(columnArray));

            performDdl(buildDDL2(columnArray));

            // expire local partition
            JdbcUtil.executeSuccess(getInnoConn(), String.format(EXPIRE, innodbTable, localPartitionQueue.poll()));

            // check correctness
            checkAgg();
            checkFilter();
            CheckIndexes();
        }
    }

}
