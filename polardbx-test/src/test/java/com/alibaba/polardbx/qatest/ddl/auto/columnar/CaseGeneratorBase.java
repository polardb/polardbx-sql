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

import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import lombok.RequiredArgsConstructor;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.nio.file.Files;
import java.util.List;
import java.util.Objects;

import static com.alibaba.polardbx.qatest.ddl.auto.partition.PartitionAutoLoadSqlTestBase.RESULT_FILE_TEMPLATE;
import static com.alibaba.polardbx.qatest.ddl.auto.partition.PartitionAutoLoadSqlTestBase.TEST_FILE_TEMPLATE;

@RequiredArgsConstructor
public abstract class CaseGeneratorBase implements Runnable {
    protected static final Logger LOGGER = LoggerFactory.getLogger(CaseGeneratorBase.class);

    protected static final String DROP_TABLE_TMPL = "DROP TABLE IF EXISTS %s;\n";
    protected static final String DROP_INDEX_TMPL = "DROP INDEX `%s` ON `%s`;\n";

    private final String testFileName;
    private final String resultFilePath;

    @Override
    public void run() {
        final String test = generateTest();
        final String result = generateResult();

        // Fill test content to file
        System.out.printf(test);
        writeFileContent(test, testFileName);

        // Fill result content to file
        System.out.println(result);
        writeFileContent(result, resultFilePath);
    }

    abstract String generateTest();

    abstract String generateResult();

    @NotNull
    protected static String generateTestFilePath(@NotNull Class testClass, @NotNull String testCaseName) {
        return String.format(TEST_FILE_TEMPLATE, testClass.getSimpleName(), testCaseName);
    }

    @NotNull
    protected static String generateResultFilePath(@NotNull Class testClass, @NotNull String testCaseName) {
        return String.format(RESULT_FILE_TEMPLATE, testClass.getSimpleName(), testCaseName);
    }

    @NotNull
    protected static String generateDropTable(@NotNull String tableNamePrefix, int tableCount) {
        final StringBuilder dropTableBuilder = new StringBuilder();
        for (int i = 0; i < tableCount; i++) {
            dropTableBuilder.append(
                String.format(DROP_TABLE_TMPL, tableNamePrefix + i));
        }
        return dropTableBuilder.toString();
    }

    @NotNull
    protected static String buildDropIndex(@NotNull String tableName, @NotNull String indexName) {
        return String.format(DROP_INDEX_TMPL, indexName, tableName);
    }

    @NotNull
    protected static String format(String sql) {
        final List<SQLStatement> sqlStatements = FastsqlUtils.parseSql(sql);
        final StringBuilder result = new StringBuilder();
        for (SQLStatement stmt : sqlStatements) {
            result.append(stmt.toString());
        }
        return result.append("\n").toString();
    }

    private boolean writeFileContent(String newResult, String fileName) {
        OutputStream out = null;
        File file = null;
        try {
            URL url = this.getClass().getClassLoader().getResource(fileName);
            file = new File(Objects.requireNonNull(url).toURI());
            out = Files.newOutputStream(file.toPath());
            OutputStreamWriter outputStreamWriter = new OutputStreamWriter(out);
            outputStreamWriter.write(newResult);
            outputStreamWriter.flush();
            return true;
        } catch (Throwable ex) {
            // ignore
            ex.printStackTrace();
            LOGGER.error(ex.getMessage());
        } finally {
            try {
                if (out != null) {
                    out.close();
                }
            } catch (IOException e) {
                // ignore
                e.printStackTrace();
                LOGGER.error(e.getMessage());
            }
        }
        return false;
    }
}
