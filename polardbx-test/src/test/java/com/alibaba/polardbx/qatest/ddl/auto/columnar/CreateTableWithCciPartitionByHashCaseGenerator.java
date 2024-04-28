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

public class CreateTableWithCciPartitionByHashCaseGenerator extends CciCaseGenerator {
    private static final String FILE_PREFIX = "test_create_table_with_cci_partition_by_hash";

    private static final String TABLE_NAME_PREFIX = "t_order_";
    private static final String INDEX_NAME_PREFIX = "cci_";
    private static final String INDEX_NAME_RESULT_TMPL = "/* %s_$ */ `%s`";
    private static final String SORT_KEY = "seller_id";
    private static final String CREATE_TABLE_TMPL = "CREATE TABLE `%s` (\n"
        + "\t`id` bigint(11) NOT NULL AUTO_INCREMENT,\n"
        + "\t`order_id` varchar(20) DEFAULT NULL,\n"
        + "\t`buyer_id` varchar(20) DEFAULT NULL,\n"
        + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
        + "\t`order_snapshot` longtext,\n"
        + "\t`order_detail` longtext,\n"
        + "\t`order_datetime` datetime DEFAULT NULL,\n"
        + "\t`gmt_modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n"
        + "\t`rint` double(10, 2),\n"
        + "\tPRIMARY KEY (`id`),\n"
        + "\tCLUSTERED COLUMNAR INDEX %s(`%s`) %s\n"
        + ") ENGINE = InnoDB DEFAULT CHARSET = utf8%s;\n";
    private static final String CREATE_TABLE_RESULT_TMPL = "CREATE PARTITION TABLE `%s` (\n"
        + "\t`id` bigint(11) NOT NULL AUTO_INCREMENT,\n"
        + "\t`order_id` varchar(20) DEFAULT NULL,\n"
        + "\t`buyer_id` varchar(20) DEFAULT NULL,\n"
        + "\t`seller_id` varchar(20) DEFAULT NULL,\n"
        + "\t`order_snapshot` longtext,\n"
        + "\t`order_detail` longtext,\n"
        + "\t`order_datetime` datetime DEFAULT NULL,\n"
        + "\t`gmt_modified` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,\n"
        + "\t`rint` double(10, 2) DEFAULT NULL,\n"
        + "\tPRIMARY KEY (`id`),\n"
        + "\tCLUSTERED COLUMNAR INDEX %s (`%s`) %s\n"
        + ") ENGINE = InnoDB DEFAULT CHARSET = utf8%s\n";

    final String TABLE_PARTITION_RESULT_TMPL = "\nPARTITION BY KEY(`%s`)\n"
        + "PARTITIONS #@#\n"
        + "/* tablegroup = `tg` */";

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

    public CreateTableWithCciPartitionByHashCaseGenerator() {
        super(CreateTableWithCciTest.class, FILE_PREFIX);
    }

    @Override
    String generateTest() {
        final int tableCount = PARTITION_FUNCTION_TMPL.size();
        // Generate DROP TABLE
        final String dropTable = generateDropTable(TABLE_NAME_PREFIX, tableCount);

        // Generate CREATE TABLE and CHECK COLUMNAR INDEX
        final StringBuilder middlePartBuilder = new StringBuilder();
        for (int i = 0; i < tableCount; i++) {
            // Table and index name
            final String tableName = TABLE_NAME_PREFIX + i;
            final String indexName = INDEX_NAME_PREFIX + i;

            // Partition part of index
            final Pair<String, Integer> partitionTmpl = PARTITION_FUNCTION_TMPL.get(i);
            final String partitionColumn = PARTITION_COLUMN.get(partitionTmpl.getValue());
            final String indexPartition = TStringUtil.isBlank(partitionTmpl.getKey()) ?
                "" : String.format(partitionTmpl.getKey(), partitionColumn);

            // Fixed index column
            final String indexColumn = SORT_KEY;

            // Auto partition table
            final String tablePartition = "";

            // Comment
            final String comment = String.format(
                "\n# create auto partition table %s with cci %s(%s) %s\n",
                tableName,
                indexName,
                indexColumn,
                indexPartition);

            // Assemble CREATE TABLE statement
            final String createTable = String.format(
                CREATE_TABLE_TMPL,
                tableName,
                indexName,
                indexColumn,
                indexPartition,
                tablePartition);

            middlePartBuilder
                .append(comment)
                .append(buildSkipRealCreateHint())
                .append(createTable)
                .append(buildShowFullCreateTableTest(tableName))
                .append(buildCheckCciMetaTest(tableName, indexName));
        }

        return "#clean\n" + dropTable + "\n" + middlePartBuilder + "\n" + "#cleanup\n" + dropTable;
    }

    @Override
    String generateResult() {
        final int tableCount = PARTITION_FUNCTION_TMPL.size();
        // Generate DROP TABLE
        final String dropTable = generateDropTable(TABLE_NAME_PREFIX, tableCount);

        // Generate CREATE TABLE and CHECK COLUMNAR INDEX
        final StringBuilder middlePartBuilder = new StringBuilder();
        for (int i = 0; i < tableCount; i++) {
            // Test and result table name
            final String tableName = TABLE_NAME_PREFIX + i;

            // Test and result index name (result index name has a random suffix)
            final String indexName = INDEX_NAME_PREFIX + i;
            final String indexNameResult = String.format(INDEX_NAME_RESULT_TMPL, indexName, indexName);

            // Test index partition part
            final Pair<String, Integer> partitionTmpl = PARTITION_FUNCTION_TMPL.get(i);
            final String partitionColumn = PARTITION_COLUMN.get(partitionTmpl.getValue());
            final String indexPartition = TStringUtil.isBlank(partitionTmpl.getKey()) ?
                "" : String.format(partitionTmpl.getKey(), partitionColumn);

            // Result index partition part
            final Pair<String, Integer> partitionResultTmpl = PARTITION_FUNCTION_RESULT_TMPL.get(i);
            final String resultPartitionColumn = PARTITION_COLUMN
                .get(partitionResultTmpl.getValue())
                .replaceAll(" ", "");
            final String indexPartitionResult = TStringUtil.isBlank(partitionResultTmpl.getKey()) ?
                "" : String.format(partitionResultTmpl.getKey(), resultPartitionColumn);

            // Fixed index column name
            final String indexColumn = SORT_KEY;

            // Test and result table partition part (auto partition mode)
            final String tablePartition = "";
            final String tablePartitionResult = String.format(TABLE_PARTITION_RESULT_TMPL, "id");

            // Test create table
            final String createTable = String.format(
                CREATE_TABLE_TMPL,
                tableName,
                indexName,
                indexColumn,
                indexPartition,
                tablePartition);

            // Result create table
            final String createTableResult = String.format(
                CREATE_TABLE_RESULT_TMPL,
                tableName,
                indexNameResult,
                indexColumn,
                indexPartitionResult,
                tablePartitionResult);

            // FastSQL will remove all comment except first one
            final String comment = String.format(
                "\n# create auto partition table %s with cci %s(%s) %s\n",
                tableName,
                indexName,
                indexColumn,
                indexPartition);

            middlePartBuilder
                .append(comment)
                .append(buildSkipRealCreateHint())
                .append(createTable)
                .append(buildShowFullCreateTableTest(tableName))
                .append(buildShowFullCreateTableResult(tableName, createTableResult))
                .append(buildCheckCciMetaTest(tableName, indexName))
                .append(buildCheckCciMetaResult(tableName, indexName));
        }

        return dropTable + "\n" + middlePartBuilder + "\n" + dropTable;
    }

    public static void main(String[] args) {
        /*
         cat ./polardbx-test/target/test-classes/partition/env/CreateTableWithCciTest/test_create_table_with_cci_partition_by_hash.test.yml > ./polardbx-test/src/test/resources/partition/env/CreateTableWithCciTest/test_create_table_with_cci_partition_by_hash.test.yml && cat ./polardbx-test/target/test-classes/partition/env/CreateTableWithCciTest/test_create_table_with_cci_partition_by_hash.result > ./polardbx-test/src/test/resources/partition/env/CreateTableWithCciTest/test_create_table_with_cci_partition_by_hash.result
         */
        final CreateTableWithCciPartitionByHashCaseGenerator gen = new CreateTableWithCciPartitionByHashCaseGenerator();
        gen.run();
    }
}
