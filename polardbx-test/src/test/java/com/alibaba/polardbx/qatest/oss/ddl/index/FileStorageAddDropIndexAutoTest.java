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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.commons.collections.CollectionUtils;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class FileStorageAddDropIndexAutoTest extends FileStorageAddDropIndexTest {
    /**
     * | ADD {INDEX | KEY} [index_name]
     * [index_type] (key_part,...) [index_option] ...
     * | DROP {INDEX | KEY} index_name
     * | DROP PRIMARY KEY
     */

    protected static List<String> IndexableColumns = ImmutableList.of(
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

    public FileStorageAddDropIndexAutoTest(String crossSchema, String seed) {
        super(crossSchema, seed);
        this.auto = true;
    }

    @Parameterized.Parameters(name = "{index}:cross={0}")
    public static List<String[]> prepareData() {
        return Arrays.asList(
            new String[] {"false", "11"},
            new String[] {"true", "12"}
        );
    }

    protected Set<String> buildIndexColumns(String[] columnArray) {
        ImmutableSet.Builder<String> builder = ImmutableSet.builder();
        for (int i = 0; i < 2; i++) {
            String column = columnArray[r1.nextInt(columnArray.length)];
            if (FileStorageAddDropIndexAutoTest.IndexableColumns.contains(column)) {
                builder.add(column);
            }
        }
        return builder.build();
    }

    protected String buildDDL1(String[] columnArray) {
        Set<String> indexColumns = buildIndexColumns(columnArray);
        if (CollectionUtils.isEmpty(indexColumns)) {
            return null;
        }
        StringBuilder ddl = new StringBuilder("alter table %s ");
        ddl.append("add index index_").append(++indexCount)
            .append("(").append(String.join(",", indexColumns)).append(")");
        return ddl.toString();
    }

    protected String buildDDL2(String[] columnArray) {
        Set<String> indexes = getIndexes(getInnoConn(), innodbTable);
        if (indexes.isEmpty()) {
            return null;
        }
        String droppedIndex = (String) indexes.toArray()[r1.nextInt(indexes.size())];
        return "alter table %s " + " drop index " + droppedIndex;
    }

}
