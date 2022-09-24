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
import java.util.List;

public class JavaFunctionTest {
    private Connection tddlCon;

    private String SqlFilePath = "src/test/java/com/alibaba/polardbx/qatest/ddl/userDefinedJavaFunction/JavaFunctionTestSql.txt";
    private File sqlFile= null;
    private BufferedReader reader = null;
    
    private int functionLines = 5;
    
    
    private String InsertTemplet =
        "insert into test (id, col1, col2, col3, col4, col5, col6, col7) values (? ,?, ?, ?, ?, ?,?,?)";
    
    @Before
    public void setup() {
        try {
            this.tddlCon = conn();
            sqlFile = new File(SqlFilePath);
            reader = new BufferedReader(new FileReader(sqlFile));
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @After
    public void after() {
        try {
            PreparedStatement dropps = tddlCon.prepareStatement("drop database if exists testdb3");
            dropps.execute();
            for (int i = 0; i < functionLines; i++) {
                PreparedStatement p = tddlCon.prepareStatement(reader.readLine());
                p.execute();
            }
            tddlCon.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static Connection conn() {
        Connection conn = null;
        String url = "jdbc:mysql://127.0.0.1:8527/";        //数据库地址
        final String username = "polardbx_root";        //数据库用户名
        final String driver = "com.mysql.jdbc.Driver";        //mysql驱动

        try {
            Class.forName(driver);  //加载数据库驱动
            try {
                conn = DriverManager.getConnection(url, username, "");  //连接数据库
            } catch (SQLException e) {
                e.printStackTrace();
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return conn;
    }

    @Test
    public void testCreateFunction() {
        try {
            JdbcUtil.createDatabase(tddlCon, "testdb3", "");
            JdbcUtil.executeQuery("use testdb3", tddlCon);

            String createTable = reader.readLine();
            PreparedStatement createTStmt = tddlCon.prepareStatement(createTable);
            reader.readLine();
            createTStmt.execute();

            PreparedStatement insertstmt = tddlCon.prepareStatement(InsertTemplet);
            insertstmt.setInt(1, 1);
            insertstmt.setString(2, "test");
            insertstmt.setBigDecimal(3, new BigDecimal("10.1"));
            insertstmt.setBigDecimal(4, new BigDecimal("15.5"));
            insertstmt.setString(5, "value1");
            insertstmt.setDate(6, new Date(1137075575000L));
            insertstmt.setTimestamp(7, new Timestamp(Long.parseLong("1137075575000")));
            Blob b = tddlCon.createBlob();
            b.setBytes(1, "test".getBytes());
            insertstmt.setBlob(8, b);

            insertstmt.execute();

            for (int i = 0; i < functionLines; i++) {
                PreparedStatement p = tddlCon.prepareStatement(reader.readLine());
                p.execute();
            }
            reader.readLine();

            for (int i = 0; i < functionLines; i++) {
                PreparedStatement p1 = tddlCon.prepareStatement(reader.readLine());
                p1.execute();
                PreparedStatement p2 = tddlCon.prepareStatement(reader.readLine());
                ResultSet rs = p2.executeQuery();
                String r = null;
                while (rs.next()) {
                    r = rs.getObject(reader.readLine()).toString();
                }
                Assert.assertEquals(reader.readLine(), r);
                reader.readLine();
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }
}
