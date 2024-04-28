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

public class CreateCciPartitionByListCaseGenerator extends CreateCciCaseGeneratorBase {
    private static final String FILE_PREFIX = "test_create_cci_partition_by_list";

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

    public CreateCciPartitionByListCaseGenerator() {
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
         cat ./polardbx-test/target/test-classes/partition/env/CreateCciTest/test_create_cci_partition_by_list.test.yml > ./polardbx-test/src/test/resources/partition/env/CreateCciTest/test_create_cci_partition_by_list.test.yml && cat ./polardbx-test/target/test-classes/partition/env/CreateCciTest/test_create_cci_partition_by_list.result > ./polardbx-test/src/test/resources/partition/env/CreateCciTest/test_create_cci_partition_by_list.result
         */
        final CreateCciPartitionByListCaseGenerator gen = new CreateCciPartitionByListCaseGenerator();
        gen.run();
    }
}
