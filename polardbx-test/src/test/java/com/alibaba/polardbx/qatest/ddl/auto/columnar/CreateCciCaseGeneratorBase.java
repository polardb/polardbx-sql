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

public abstract class CreateCciCaseGeneratorBase extends CciCaseGenerator {
    protected static final String TABLE_NAME_PREFIX = "t_order_";
    protected static final String INDEX_NAME_PREFIX = "cci_";
    protected static final String SORT_KEY = "seller_id";
    protected static final String INDEX_NAME_RESULT_TMPL = "/* %s_$ */ `%s`";
    protected static final String CREATE_TABLE_TMPL = "CREATE TABLE `%s` (\n"
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
        + "\tPRIMARY KEY (`id`)\n"
        + ") ENGINE = InnoDB DEFAULT CHARSET = utf8%s;\n";
    protected static final String CREATE_TABLE_RESULT_TMPL = "CREATE PARTITION TABLE `%s` (\n"
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

    protected static final String TABLE_PARTITION_RESULT_TMPL = "\nPARTITION BY KEY(`%s`)\n"
        + "PARTITIONS #@#\n"
        + "/* tablegroup = `tg` */";

    protected static final String CREATE_CCI_TMPL = "CREATE CLUSTERED COLUMNAR INDEX %s ON %s (`%s`) %s;\n";
    protected static final String ALTER_TABLE_ADD_CCI_TMPL =
        "ALTER TABLE `%s` ADD CLUSTERED COLUMNAR INDEX %s (`%s`) %s;\n";

    public CreateCciCaseGeneratorBase(Class testClass, String testFilePrefix) {
        super(testClass, testFilePrefix);
    }

    protected abstract int getCciCount();

    protected abstract String getTableNamePrefix();

    protected abstract String getIndexNamePrefix();

    protected abstract String getIndexColumn(int index);

    protected abstract String buildIndexPartitionTest(int index);

    protected abstract String buildCreateTableTest(String tableName);

    protected abstract String buildCreateCci(String tableName, String indexName, int index);

    protected abstract String buildComment(String tableName, String indexName, int index);

    protected abstract String buildIndexNameResult(String indexName);

    protected abstract String buildIndexPartitionResult(int index);

    protected abstract String buildTablePartitionResult(int index);

    protected abstract String buildCreateTableResult(String tableName, String indexName, int index);

    @Override
    String generateTest() {
        final int cciCount = getCciCount();
        // Generate DROP TABLE
        final String dropTable = generateDropTable(getTableNamePrefix(), 1);

        // Generate CREATE TABLE
        final String tableName = getTableNamePrefix() + "0";
        // Auto partition table
        final String createTable = buildCreateTableTest(tableName);

        // Generate CREATE CCI and CHECK CCI
        final StringBuilder middlePartBuilder = new StringBuilder();
        for (int i = 0; i < cciCount; i++) {
            // Table and index name
            final String indexName = getIndexNamePrefix() + i;

            // Comment
            final String comment = buildComment(tableName, indexName, i);

            // Assemble CREATE INDEX statement
            final String createCci = buildCreateCci(tableName, indexName, i);

            middlePartBuilder
                .append(comment)
                .append(buildSkipRealCreateHint())
                .append(createCci)
                .append(buildAlterCciStatus(tableName, indexName))
                .append(buildShowFullCreateTableTest(tableName))
                .append(buildCheckCciMetaTest(tableName, indexName))
                .append(buildDropIndex(tableName, indexName));
        }

        return "#clean\n" + dropTable + "\n" + createTable + "\n" + middlePartBuilder + "\n" + "#cleanup\n" + dropTable;
    }

    @Override
    String generateResult() {
        final int tableCount = getCciCount();
        // Generate DROP TABLE
        final String dropTable = generateDropTable(getTableNamePrefix(), 1);

        // Generate CREATE TABLE
        final String tableName = getTableNamePrefix() + "0";
        // Auto partition table
        final String createTable = buildCreateTableTest(tableName);

        // Generate CREATE TABLE and CHECK COLUMNAR INDEX
        final StringBuilder middlePartBuilder = new StringBuilder();
        for (int i = 0; i < tableCount; i++) {
            // Test and result index name (result index name has a random suffix)
            final String indexName = getIndexNamePrefix() + i;

            // Test create index
            final String createCci = buildCreateCci(tableName, indexName, i);
            final String formattedCreateCci = format(createCci);

            // Result create table
            final String createTableResult = buildCreateTableResult(tableName, indexName, i);

            // FastSQL will remove all comment except first one
            final String comment = buildComment(tableName, indexName, i);

            middlePartBuilder
                .append(comment)
                .append(buildSkipRealCreateHint())
                .append(formattedCreateCci)
                .append(buildAlterCciStatus(tableName, indexName))
                .append(buildShowFullCreateTableTest(tableName))
                .append(buildShowFullCreateTableResult(tableName, createTableResult))
                .append(buildCheckCciMetaTest(tableName, indexName))
                .append(buildCheckCciMetaResult(tableName, indexName))
                .append(buildDropIndex(tableName, indexName));
        }

        return dropTable + "\n" + createTable + "\n" + middlePartBuilder + "\n" + dropTable;
    }
}
