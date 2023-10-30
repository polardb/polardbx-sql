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

import org.apache.commons.collections.CollectionUtils;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class FileStorageCreateDropIndexTest extends FileStorageAddDropIndexTest {
    /**
     * CREATE [UNIQUE | FULLTEXT | SPATIAL] INDEX index_name
     * [index_type]
     * ON tbl_name (key_part,...)
     * [index_option]
     * [algorithm_option | lock_option] ...
     * <p>
     * DROP INDEX index_name ON tbl_name
     * [algorithm_option | lock_option] ...
     */
    public FileStorageCreateDropIndexTest(String crossSchema, String seed) {
        super(crossSchema, seed);
    }

    @Parameterized.Parameters(name = "{index}:cross={0}")
    public static List<String[]> prepareData() {
        return Arrays.asList(
            new String[] {"false", "9"},
            new String[] {"true", "10"}
        );
    }

    protected void addIndex(Set<String> columns) {
        if (CollectionUtils.isEmpty(columns)) {
            return;
        }
        String ddl = "create index index_" + (++indexCount) + " on %s ("
            + String.join(",", columns) + ")";
        performDdl(ddl);
    }

    protected String buildDDL1(String[] columnArray) {
        Set<String> indexColumns = buildIndexColumns(columnArray);
        if (CollectionUtils.isEmpty(indexColumns)) {
            return null;
        }
        StringBuilder ddl = new StringBuilder("create index ");
        if (!CollectionUtils.isEmpty(indexColumns)) {
            ddl.append("index_").append(++indexCount)
                .append(" on %s (").append(String.join(",", indexColumns)).append(")");
        }
        return ddl.toString();
    }

    protected String buildDDL2(String[] columnArray) {
        Set<String> indexes = getIndexes(getInnoConn(), innodbTable);
        if (indexes.isEmpty()) {
            return null;
        }
        String droppedIndex = (String) indexes.toArray()[r1.nextInt(indexes.size())];
        return " drop index " + droppedIndex + " on %s";
    }

}
