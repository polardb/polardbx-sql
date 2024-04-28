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

public class CreateCciPartitionByRangeCaseGenerator extends CreateCciCaseGeneratorBase {
    private static final String FILE_PREFIX = "test_create_cci_partition_by_range";

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
        PARTITION_COLUMN.add("`order_id`, `order_datetime`");
        PARTITION_COLUMN.add("order_datetime");

        PARTITION_FUNCTION_TMPL.add(Pair.of("PARTITION BY RANGE COLUMNS (%s) (\n"
            + "\t\tPARTITION p1 VALUES LESS THAN (10000, '2021-01-01'),\n"
            + "\t\tPARTITION p2 VALUES LESS THAN (20000, '2021-01-01'),\n"
            + "\t\tPARTITION p3 VALUES LESS THAN (30000, '2021-01-01'),\n"
            + "\t\tPARTITION p4 VALUES LESS THAN (40000, '2021-01-01'),\n"
            + "\t\tPARTITION p5 VALUES LESS THAN (50000, '2021-01-01'),\n"
            + "\t\tPARTITION p6 VALUES LESS THAN (MAXVALUE, MAXVALUE)\n"
            + "\t)", 0));
        PARTITION_FUNCTION_TMPL.add(Pair.of("PARTITION BY RANGE (to_days(`%s`)) (\n"
            + "\t\tPARTITION p1 VALUES LESS THAN (to_days('2021-01-01')),\n"
            + "\t\tPARTITION p2 VALUES LESS THAN (to_days('2021-04-01')),\n"
            + "\t\tPARTITION p3 VALUES LESS THAN (to_days('2021-07-01')),\n"
            + "\t\tPARTITION p4 VALUES LESS THAN (to_days('2021-10-01')),\n"
            + "\t\tPARTITION p5 VALUES LESS THAN (to_days('2022-01-01')),\n"
            + "\t\tPARTITION p6 VALUES LESS THAN MAXVALUE\n"
            + "\t)", 1));

        PARTITION_FUNCTION_RESULT_TMPL.add(Pair.of("\n\t\tPARTITION BY RANGE COLUMNS(%s)\n"
            + "\t\t(PARTITION p1 VALUES LESS THAN ('10000','2021-01-01 00:00:00') ENGINE = InnoDB,\n"
            + "\t\t PARTITION p2 VALUES LESS THAN ('20000','2021-01-01 00:00:00') ENGINE = InnoDB,\n"
            + "\t\t PARTITION p3 VALUES LESS THAN ('30000','2021-01-01 00:00:00') ENGINE = InnoDB,\n"
            + "\t\t PARTITION p4 VALUES LESS THAN ('40000','2021-01-01 00:00:00') ENGINE = InnoDB,\n"
            + "\t\t PARTITION p5 VALUES LESS THAN ('50000','2021-01-01 00:00:00') ENGINE = InnoDB,\n"
            + "\t\t PARTITION p6 VALUES LESS THAN (MAXVALUE,MAXVALUE) ENGINE = InnoDB)", 0));
        PARTITION_FUNCTION_RESULT_TMPL.add(Pair.of("\n\t\tPARTITION BY RANGE(TO_DAYS(`%s`))\n"
            + "\t\t(PARTITION p1 VALUES LESS THAN (738156) ENGINE = InnoDB,\n"
            + "\t\t PARTITION p2 VALUES LESS THAN (738246) ENGINE = InnoDB,\n"
            + "\t\t PARTITION p3 VALUES LESS THAN (738337) ENGINE = InnoDB,\n"
            + "\t\t PARTITION p4 VALUES LESS THAN (738429) ENGINE = InnoDB,\n"
            + "\t\t PARTITION p5 VALUES LESS THAN (738521) ENGINE = InnoDB,\n"
            + "\t\t PARTITION p6 VALUES LESS THAN (MAXVALUE) ENGINE = InnoDB)", 1));
    }

    public CreateCciPartitionByRangeCaseGenerator() {
        super(CreateCciTest.class, FILE_PREFIX);
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
            CREATE_CCI_TMPL,
            indexName,
            tableName,
            getIndexColumn(index),
            buildIndexPartitionTest(index));
    }

    @Override
    protected String buildComment(String tableName, String indexName, int index) {
        return String.format(
            "\n# create cci %s(%s) %s on auto partition table %s\n",
            indexName,
            getIndexColumn(index),
            buildIndexPartitionTest(index).replace("\n", "\n#"),
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
         cat ./polardbx-test/target/test-classes/partition/env/CreateCciTest/test_create_cci_partition_by_range.test.yml > ./polardbx-test/src/test/resources/partition/env/CreateCciTest/test_create_cci_partition_by_range.test.yml && cat ./polardbx-test/target/test-classes/partition/env/CreateCciTest/test_create_cci_partition_by_range.result > ./polardbx-test/src/test/resources/partition/env/CreateCciTest/test_create_cci_partition_by_range.result
         */
        final CreateCciPartitionByRangeCaseGenerator gen =
            new CreateCciPartitionByRangeCaseGenerator();
        gen.run();
    }
}
