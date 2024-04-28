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

public class CreateTableWithCciPartitionByListCaseGenerator extends CciCaseGenerator {
    private static final String FILE_PREFIX = "test_create_table_with_cci_partition_by_list";

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
        + "\t`country` varchar(64) DEFAULT NULL,\n"
        + "\t`city` varchar(64) DEFAULT NULL,\n"
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
        + "\t`country` varchar(64) DEFAULT NULL,\n"
        + "\t`city` varchar(64) DEFAULT NULL,\n"
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
        PARTITION_COLUMN.add("`country`, `city`");
        PARTITION_COLUMN.add("order_datetime");

        PARTITION_FUNCTION_TMPL.add(Pair.of("PARTITION BY LIST COLUMNS (%s) (\n"
            + "\t\tPARTITION p1 VALUES IN (('China', 'Hangzhou'), ('China', 'Beijing')),\n"
            + "\t\tPARTITION p2 VALUES IN (('United States', 'NewYork'), ('United States', 'Chicago')),\n"
            + "\t\tPARTITION p3 VALUES IN (('Russian', 'Moscow'))\n"
            + "\t)\n", 0));
        PARTITION_FUNCTION_TMPL.add(Pair.of("PARTITION BY LIST (YEAR(`%s`)) (\n"
            + "\t\tPARTITION p1 VALUES IN (1990, 1991, 1992, 1993, 1994, 1995, 1996, 1997, 1998, 1999),\n"
            + "\t\tPARTITION p2 VALUES IN (2000, 2001, 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2009),\n"
            + "\t\tPARTITION p3 VALUES IN (2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019)\n"
            + "\t)", 1));

        PARTITION_FUNCTION_RESULT_TMPL.add(Pair.of("\n\t\tPARTITION BY LIST COLUMNS(%s)\n"
                + "\t\t(PARTITION p1 VALUES IN (('China','Beijing'),('China','Hangzhou')) ENGINE = InnoDB,\n"
                + "\t\t PARTITION p3 VALUES IN (('Russian','Moscow')) ENGINE = InnoDB,\n"
                + "\t\t PARTITION p2 VALUES IN (('United States','Chicago'),('United States','NewYork')) ENGINE = InnoDB)",
            0));
        PARTITION_FUNCTION_RESULT_TMPL.add(Pair.of("\n\t\tPARTITION BY LIST(YEAR(`%s`))\n"
            + "\t\t(PARTITION p1 VALUES IN (1990,1991,1992,1993,1994,1995,1996,1997,1998,1999) ENGINE = InnoDB,\n"
            + "\t\t PARTITION p2 VALUES IN (2000,2001,2002,2003,2004,2005,2006,2007,2008,2009) ENGINE = InnoDB,\n"
            + "\t\t PARTITION p3 VALUES IN (2010,2011,2012,2013,2014,2015,2016,2017,2018,2019) ENGINE = InnoDB)", 1));
    }

    public CreateTableWithCciPartitionByListCaseGenerator() {
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
                indexPartition.replace("\n", "\n#"));

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
                indexPartition.replace("\n", "\n#"));

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
         cat ./polardbx-test/target/test-classes/partition/env/CreateTableWithCciTest/test_create_table_with_cci_partition_by_list.test.yml > ./polardbx-test/src/test/resources/partition/env/CreateTableWithCciTest/test_create_table_with_cci_partition_by_list.test.yml && cat ./polardbx-test/target/test-classes/partition/env/CreateTableWithCciTest/test_create_table_with_cci_partition_by_list.result > ./polardbx-test/src/test/resources/partition/env/CreateTableWithCciTest/test_create_table_with_cci_partition_by_list.result
         */
        final CreateTableWithCciPartitionByListCaseGenerator gen = new CreateTableWithCciPartitionByListCaseGenerator();
        gen.run();
    }
}
