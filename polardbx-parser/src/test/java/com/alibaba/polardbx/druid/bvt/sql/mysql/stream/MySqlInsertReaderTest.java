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

package com.alibaba.polardbx.druid.bvt.sql.mysql.stream;

import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.parser.MySqlInsertReader;
import junit.framework.TestCase;

import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class MySqlInsertReaderTest extends TestCase {
    public void test_for_reader() throws Exception {
        String resource = "bvt/parser/mysql_large_insert_0.sql.gz";
        InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(resource);
        assertNotNull(is);

        GZIPInputStream zis = new GZIPInputStream(is);
        Reader reader = new InputStreamReader(zis);

        MySqlInsertReader insertReader = new MySqlInsertReader(reader);
        MySqlInsertStatement statement = insertReader.parseStatement();

//        StringBuilder buf = new StringBuilder();
//        while (insertReader.isEOF()) {
//            SQLInsertStatement.ValuesClause clause = insertReader.readCaluse();
//            SQLIntegerExpr integerExpr = (SQLIntegerExpr) clause.getValues().get(0);
//            Number fid = integerExpr.getNumber();
//
//            String str = clause.getOriginalString();
//            buf.append(str);
//        }

        System.out.println(statement);

    }

    public void f_test_gen() throws Exception {
        OutputStream fos = new GZIPOutputStream(new FileOutputStream("/Users/wenshao/tmp/mysql_large_insert_0.sql.gz"));
        Writer writer = new OutputStreamWriter(fos);
        writer.write("insert into t_user (fid, fname) values \n");
        for (int i = 0; i < 1000 * 100; ++i) {
            if (i != 0) {
                writer.write("\n, ");
            }

            int val = 1000 * 1000 * 1 + i;
            String id = Integer.toString(val);
            writer.write('(');
            writer.write(id);
            writer.write(", 'v_");
            writer.write(id);
            writer.write("')");
        }
        writer.flush();
        writer.close();
    }
}
