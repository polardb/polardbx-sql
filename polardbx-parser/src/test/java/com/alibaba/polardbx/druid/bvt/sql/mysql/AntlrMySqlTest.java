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

package com.alibaba.polardbx.druid.bvt.sql.mysql;

import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.visitor.SchemaStatVisitor;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import junit.framework.TestCase;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.net.URL;
import java.util.List;

public class AntlrMySqlTest extends TestCase {
    public void test_for_antlr_examples() throws Exception {
        SchemaStatVisitor schemaStatVisitor = SQLUtils.createSchemaStatVisitor(JdbcConstants.MYSQL);

        String path = "bvt/parser/antlr_grammers_v4_mysql/examples/";
        URL resource = Thread.currentThread().getContextClassLoader().getResource(path);
        File dir = new File(resource.getFile());
        for (File file : dir.listFiles()) {
            System.out.println(file);
            String sql = FileUtils.readFileToString(file);
            System.out.println(sql);
            List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, JdbcConstants.MYSQL);
            for (SQLStatement stmt : stmtList) {
                stmt.toString();

                stmt.accept(schemaStatVisitor);
            }
        }
    }
}
