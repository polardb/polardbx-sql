/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.qatest.ddl.auto.columnar;

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.TStringUtil;

import java.util.ArrayList;
import java.util.List;

public class AlterTableAddCciPartitionByHashCaseGenerator extends CreateCciCaseGeneratorBase {
    private static final String FILE_PREFIX = "test_alter_table_add_cci_partition_by_hash";

    /**
     * <partition_option, partition_column>
     */
    private static final List<Pair<String, Integer>> PARTITION_FUNCTION_TMPL = new ArrayList<>();
    /**
     * <result_partition_option, partition_column>
     */
    private static final List<Pair<String, Integer>> PARTITION_FUNCTION_RESULT_TMPL = new ArrayList<>();
    private static final List<String> PARTITION_COLUMN = new ArrayList<>();

    static {
        PARTITION_COLUMN.add("seller_id");
        PARTITION_COLUMN.add("`buyer_id`, `order_id`");
        PARTITION_COLUMN.add("order_datetime");
        PARTITION_COLUMN.add("gmt_modified");
        PARTITION_COLUMN.add("id");

        PARTITION_FUNCTION_TMPL.add(Pair.of("PARTITION BY KEY (`%s`) PARTITIONS 3", 0));
        PARTITION_FUNCTION_TMPL.add(Pair.of("PARTITION BY KEY (%s) PARTITIONS 3", 1));
        PARTITION_FUNCTION_TMPL.add(Pair.of("PARTITION BY HASH (`%s`) PARTITIONS 3", 0));
        PARTITION_FUNCTION_TMPL.add(Pair.of("PARTITION BY HASH (%s) PARTITIONS 3", 1));
        PARTITION_FUNCTION_TMPL.add(Pair.of("PARTITION BY HASH (YEAR(`%s`)) PARTITIONS 3", 2));
        PARTITION_FUNCTION_TMPL.add(Pair.of("PARTITION BY HASH (MONTH(`%s`)) PARTITIONS 3", 2));
        PARTITION_FUNCTION_TMPL.add(Pair.of("PARTITION BY HASH (DAYOFMONTH(`%s`)) PARTITIONS 3", 2));
        PARTITION_FUNCTION_TMPL.add(Pair.of("PARTITION BY HASH (DAYOFWEEK(`%s`)) PARTITIONS 3", 2));
        PARTITION_FUNCTION_TMPL.add(Pair.of("PARTITION BY HASH (DAYOFYEAR(`%s`)) PARTITIONS 3", 2));
        PARTITION_FUNCTION_TMPL.add(Pair.of("PARTITION BY HASH (TO_DAYS(`%s`)) PARTITIONS 3", 2));
        PARTITION_FUNCTION_TMPL.add(Pair.of("PARTITION BY HASH (TO_MONTHS(`%s`)) PARTITIONS 3", 2));
        PARTITION_FUNCTION_TMPL.add(Pair.of("PARTITION BY HASH (TO_WEEKS(`%s`)) PARTITIONS 3", 2));
        PARTITION_FUNCTION_TMPL.add(Pair.of("PARTITION BY HASH (TO_SECONDS(`%s`)) PARTITIONS 3", 2));
        PARTITION_FUNCTION_TMPL.add(Pair.of("PARTITION BY HASH (UNIX_TIMESTAMP(`%s`)) PARTITIONS 3", 3));
        PARTITION_FUNCTION_TMPL.add(Pair.of("PARTITION BY HASH (SUBSTR(`%s`, 1, 4)) PARTITIONS 3", 0));

        PARTITION_FUNCTION_TMPL.add(Pair.of("PARTITION BY KEY (`%s`)", 0));
        PARTITION_FUNCTION_TMPL.add(Pair.of("PARTITION BY HASH (`%s`)", 0));
        PARTITION_FUNCTION_TMPL.add(Pair.of("PARTITION BY HASH (SUBSTR(`%s`, 1, 4))", 0));

        PARTITION_FUNCTION_TMPL.add(Pair.of("", 4));

        PARTITION_FUNCTION_RESULT_TMPL.add(Pair.of("\n\t\tPARTITION BY KEY(`%s`)\n\t\tPARTITIONS 3", 0));
        PARTITION_FUNCTION_RESULT_TMPL.add(Pair.of("\n\t\tPARTITION BY KEY(%s)\n\t\tPARTITIONS 3", 1));
        PARTITION_FUNCTION_RESULT_TMPL.add(Pair.of("\n\t\tPARTITION BY HASH(`%s`)\n\t\tPARTITIONS 3", 0));
        PARTITION_FUNCTION_RESULT_TMPL.add(Pair.of("\n\t\tPARTITION BY HASH(%s)\n\t\tPARTITIONS 3", 1));
        PARTITION_FUNCTION_RESULT_TMPL.add(Pair.of("\n\t\tPARTITION BY HASH(YEAR(`%s`))\n\t\tPARTITIONS 3", 2));
        PARTITION_FUNCTION_RESULT_TMPL.add(Pair.of("\n\t\tPARTITION BY HASH(MONTH(`%s`))\n\t\tPARTITIONS 3", 2));
        PARTITION_FUNCTION_RESULT_TMPL.add(
            Pair.of("\n\t\tPARTITION BY HASH(DAYOFMONTH(`%s`))\n\t\tPARTITIONS 3", 2));
        PARTITION_FUNCTION_RESULT_TMPL.add(
            Pair.of("\n\t\tPARTITION BY HASH(DAYOFWEEK(`%s`))\n\t\tPARTITIONS 3", 2));
        PARTITION_FUNCTION_RESULT_TMPL.add(
            Pair.of("\n\t\tPARTITION BY HASH(DAYOFYEAR(`%s`))\n\t\tPARTITIONS 3", 2));
        PARTITION_FUNCTION_RESULT_TMPL.add(
            Pair.of("\n\t\tPARTITION BY HASH(TO_DAYS(`%s`))\n\t\tPARTITIONS 3", 2));
        PARTITION_FUNCTION_RESULT_TMPL.add(
            Pair.of("\n\t\tPARTITION BY HASH(TO_MONTHS(`%s`))\n\t\tPARTITIONS 3", 2));
        PARTITION_FUNCTION_RESULT_TMPL.add(
            Pair.of("\n\t\tPARTITION BY HASH(TO_WEEKS(`%s`))\n\t\tPARTITIONS 3", 2));
        PARTITION_FUNCTION_RESULT_TMPL.add(
            Pair.of("\n\t\tPARTITION BY HASH(TO_SECONDS(`%s`))\n\t\tPARTITIONS 3", 2));
        PARTITION_FUNCTION_RESULT_TMPL.add(
            Pair.of("\n\t\tPARTITION BY HASH(UNIX_TIMESTAMP(`%s`))\n\t\tPARTITIONS 3", 3));
        PARTITION_FUNCTION_RESULT_TMPL.add(
            Pair.of("\n\t\tPARTITION BY HASH(SUBSTR(`%s`,1,4))\n\t\tPARTITIONS 3", 0));

        PARTITION_FUNCTION_RESULT_TMPL.add(Pair.of("\n\t\tPARTITION BY KEY(`%s`)\n\t\tPARTITIONS 16", 0));
        PARTITION_FUNCTION_RESULT_TMPL.add(Pair.of("\n\t\tPARTITION BY HASH(`%s`)\n\t\tPARTITIONS 16", 0));
        PARTITION_FUNCTION_RESULT_TMPL.add(
            Pair.of("\n\t\tPARTITION BY HASH(SUBSTR(`%s`,1,4))\n\t\tPARTITIONS 16", 0));

        PARTITION_FUNCTION_RESULT_TMPL.add(Pair.of("\n\t\tPARTITION BY HASH(`%s`)\n\t\tPARTITIONS 16", 4));
    }

    public AlterTableAddCciPartitionByHashCaseGenerator() {
        super(AlterTableAddCciTest.class, FILE_PREFIX);
    }

    @Override
    protected int getCciCount() {
        return PARTITION_FUNCTION_TMPL.size();
    }

    @Override
    protected String getTableNamePrefix() {
        return TABLE_NAME_PREFIX;
    }

    @Override
    protected String getIndexNamePrefix() {
        return INDEX_NAME_PREFIX;
    }

    @Override
    protected String buildCreateTableTest(String tableName) {
        return String.format(CREATE_TABLE_TMPL, tableName, "");
    }

    @Override
    protected String buildIndexPartitionTest(int index) {
        // Partition part of index
        final Pair<String, Integer> partitionTmpl = PARTITION_FUNCTION_TMPL.get(index);
        final String partitionColumn = PARTITION_COLUMN.get(partitionTmpl.getValue());

        return TStringUtil.isBlank(partitionTmpl.getKey()) ?
            "" : String.format(partitionTmpl.getKey(), partitionColumn);
    }

    @Override
    protected String buildCreateCci(String tableName, String indexName, int index) {
        return String.format(
            ALTER_TABLE_ADD_CCI_TMPL,
            tableName,
            indexName,
            getIndexColumn(index),
            buildIndexPartitionTest(index));
    }

    @Override
    protected String buildComment(String tableName, String indexName, int index) {
        return String.format(
            "\n# alter table add cci %s(%s) %s on auto partition table %s\n",
            indexName,
            getIndexColumn(index),
            buildIndexPartitionTest(index),
            tableName);
    }

    @Override
    protected String buildIndexNameResult(String indexName) {
        return String.format(INDEX_NAME_RESULT_TMPL, indexName, indexName);
    }

    @Override
    protected String buildIndexPartitionResult(int index) {
        // Partition part of index
        final Pair<String, Integer> partitionResultTmpl = PARTITION_FUNCTION_RESULT_TMPL.get(index);
        final String resultPartitionColumn = PARTITION_COLUMN
            .get(partitionResultTmpl.getValue())
            .replaceAll(" ", "");

        return TStringUtil.isBlank(partitionResultTmpl.getKey()) ?
            "" : String.format(partitionResultTmpl.getKey(), resultPartitionColumn);
    }

    @Override
    protected String buildTablePartitionResult(int index) {
        return String.format(TABLE_PARTITION_RESULT_TMPL, "id");
    }

    @Override
    protected String buildCreateTableResult(String tableName, String indexName, int index) {
        return String.format(
            CREATE_TABLE_RESULT_TMPL,
            tableName,
            buildIndexNameResult(indexName),
            getIndexColumn(index),
            buildIndexPartitionResult(index),
            buildTablePartitionResult(index));
    }

    @Override
    protected String getIndexColumn(int i) {
        // Fixed index column
        return SORT_KEY;
    }

    public static void main(String[] args) {
        /*
         cat ./polardbx-test/target/test-classes/partition/env/AlterTableAddCciTest/test_alter_table_add_cci_partition_by_hash.test.yml > ./polardbx-test/src/test/resources/partition/env/AlterTableAddCciTest/test_alter_table_add_cci_partition_by_hash.test.yml && cat ./polardbx-test/target/test-classes/partition/env/AlterTableAddCciTest/test_alter_table_add_cci_partition_by_hash.result > ./polardbx-test/src/test/resources/partition/env/AlterTableAddCciTest/test_alter_table_add_cci_partition_by_hash.result
         */
        final AlterTableAddCciPartitionByHashCaseGenerator gen = new AlterTableAddCciPartitionByHashCaseGenerator();
        gen.run();
    }
}
