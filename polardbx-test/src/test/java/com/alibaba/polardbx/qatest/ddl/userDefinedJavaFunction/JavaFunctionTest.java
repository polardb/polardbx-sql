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

package com.alibaba.polardbx.qatest.ddl.userDefinedJavaFunction;

import com.alibaba.polardbx.qatest.CrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class JavaFunctionTest extends CrudBasedLockTestCase {
    private String SqlFilePath = "src/test/resources/udf/JavaFunctionTestSql.txt";
    private File sqlFile= null;
    private BufferedReader reader = null;
    
    private int functionLines = 5;
    
    
    private String InsertTemplet =
        "insert into test (id, col1, col2, col3, col4, col5, col6, col7) values (? ,?, ?, ?, ?, ?,?,?)";
    
    @Before
    public void setup() {
        try {
            sqlFile = new File(SqlFilePath);
            reader = new BufferedReader(new FileReader(sqlFile));
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @After
    public void after() {
        try {
            PreparedStatement dropps = tddlConnection.prepareStatement("drop database if exists testdb3");
            dropps.execute();
            for (int i = 0; i < functionLines; i++) {
                PreparedStatement p = tddlConnection.prepareStatement(reader.readLine());
                p.execute();
            }
            tddlConnection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testCreateFunction() {
        try {
            JdbcUtil.createDatabase(tddlConnection, "testdb3", "");
            JdbcUtil.executeQuery("use testdb3", tddlConnection);

            String createTable = reader.readLine();
            PreparedStatement createTStmt = tddlConnection.prepareStatement(createTable);
            reader.readLine();
            createTStmt.execute();

            PreparedStatement insertstmt = tddlConnection.prepareStatement(InsertTemplet);
            insertstmt.setInt(1, 1);
            insertstmt.setString(2, "test");
            insertstmt.setBigDecimal(3, new BigDecimal("10.1"));
            insertstmt.setBigDecimal(4, new BigDecimal("15.5"));
            insertstmt.setString(5, "value1");
            insertstmt.setDate(6, new Date(1137075575000L));
            insertstmt.setTimestamp(7, new Timestamp(Long.parseLong("1137075575000")));
            Blob b = tddlConnection.createBlob();
            b.setBytes(1, "test".getBytes());
            insertstmt.setBlob(8, b);

            insertstmt.execute();

            for (int i = 0; i < functionLines; i++) {
                PreparedStatement p = tddlConnection.prepareStatement(reader.readLine());
                p.execute();
            }
            reader.readLine();

            for (int i = 0; i < functionLines; i++) {
                PreparedStatement p1 = tddlConnection.prepareStatement(reader.readLine());
                p1.execute();
                PreparedStatement p2 = tddlConnection.prepareStatement(reader.readLine());
                ResultSet rs = p2.executeQuery();
                String r = null;
                while (rs.next()) {
                    r = rs.getObject(reader.readLine()).toString();
                }
                Assert.assertEquals(reader.readLine(), r);
                reader.readLine();
            }

            Set<String> realSet = new HashSet<>();
            PreparedStatement testShow = tddlConnection.prepareStatement(reader.readLine());
            ResultSet rs1 = testShow.executeQuery();
            String r1 = null;
            while (rs1.next()) {
                r1 = rs1.getString("FUNCTIONNAME");
                String s = r1;
                realSet.add(s);
            }

            Set<String> expectSet = new HashSet<>();
            for(int i = 0; i < functionLines; i++) {
                String s = reader.readLine();
                expectSet.add(s);
            }
            reader.readLine();
            Assert.assertEquals(expectSet, realSet);

        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }
}
