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

    private String CREATE_TABLE = "create table test ("
        + "id integer not null AUTO_INCREMENT primary key,\n"
        + "col1 varchar(20),\n"
        + "col2 decimal,\n"
        + "col3 numeric,\n"
        + "col4 ENUM ('value1','value2','value3'),\n"
        + "col5 date ,\n"
        + "col6 datetime ,\n"
        + "col7 blob)dbpartition by hash(id) tbpartition by hash(id) tbpartitions 3";

    private String InsertTemplet =
        "insert into test (id, col1, col2, col3, col4, col5, col6, col7) values (? ,?, ?, ?, ?, ?,?,?)";

    private List<String> createFunctionStmt = Arrays.asList(
        "CREATE function Addfour\n"
            + "returnType bigint\n"
            + "inputType bigint, bigint\n"
            + "import\n"
            + "import java.util.Date;\n"
            + "endimport\n"
            + "CODE\n"
            + "    public Object compute(Object[] args) {\n"
            + "        int a = Integer.parseInt(args[0].toString());\n"
            + "        int b = Integer.parseInt(args[1].toString());\n"
            + "\n"
            + "        return a + b;\n"
            + "    }\n"
            + "ENDCODE",
        "CREATE function testString\n"
            + "returnType varchar\n"
            + "inputType varchar\n"
            + "import\n"
            + "import java.util.Date;\n"
            + "endimport\n"
            + "CODE\n"
            + "    public Object compute(Object[] args) {\n"
            + "        String a = args[0].toString();\n"
            + "        return a + \"suffix\";\n"
            + "    }\n"
            + "ENDCODE",
        //对于enum列来说，每行数据都应该对应着一个enum值，因此可以当作varchar来处理
        "CREATE function testEnum\n"
            + "returnType Integer\n"
            + "inputType varchar\n"
            + "CODE\n"
            + "    public Object compute(Object[] args) {\n"
            + "        String a = args[0].toString();\n"
            + "        String[] enums = new String[]{\"value2\",\"value1\",\"value3\"};"
            + "         for (int i = 0; i < 3; i++) {"
            + "             if (a.equals(enums[i])) {"
            + "              return i;"
            + "             }"
            + "         }"
            + "         return -1;"
            + "    }\n"
            + "ENDCODE",
        "CREATE function testDate\n"
            + "returnType date\n"
            + "inputType date\n"
            + "import\n"
            + "import java.sql.Date;\n"
            + "endimport\n"
            + "CODE\n"
            + "    public Object compute(Object[] args) {\n"
            + "        Date a = (Date) args[0];\n"
            + "        Date b = new Date(a.getTime() + 1000*60*60*24);\n"
            + "         return b;\n"
            + "    }\n"
            + "ENDCODE",
        "CREATE function testBlob\n"
            + "returnType varchar\n"
            + "inputType blob\n"
            + "import\n"
            + "import java.sql.Blob;\n"
            + "endimport\n"
            + "CODE\n"
            + "    public Object compute(Object[] args) {\n"
            + "String s = \"\";\n"
            + "try {"
            + "        Blob b = (Blob) args[0];\n"
            + "        s = new String(b.getBytes((long)1, (int)b.length()));;\n"
            + "        \n"
            + "} catch (Exception e) {  e.printStackTrace();\n}"
            + "        return s;"
            + "    }\n"
            + "ENDCODE"
    );
    private List<String> queryString = Arrays.asList(
        "select addfour(1,2)",
        "select testString(col1) from test",
        "select testEnum(col4) from test",
        "select testDate(col5) from test",
        "select testBlob(col7) from test"
    );
    private List<String> deleteString = Arrays.asList(
        "drop function if exists addfour",
        "drop function if exists testString",
        "drop function if exists testEnum",
        "drop function if exists testDate",
        "drop function if exists testBlob"
    );
    private List<String> expectString = Arrays.asList(
        "3",
        "testsuffix",
        "1",
        "2006-01-13",
        "test"
    );
    private List<String> colIndex = Arrays.asList(
        "addfour(1, 2)",
        "testString(col1)",
        "testEnum(col4)",
        "testDate(col5)",
        "testBlob(col7)"
    );

    @Before
    public void setup() {
        this.tddlCon = conn();
    }

    @After
    public void after() {
        try {
            PreparedStatement dropps = tddlCon.prepareStatement("drop database if exists testdb");
            dropps.execute();
            for (String D : deleteString) {
                PreparedStatement p = tddlCon.prepareStatement(D);
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
            JdbcUtil.createDatabase(tddlCon, "testdb", "");
            JdbcUtil.executeQuery("use testdb", tddlCon);
            PreparedStatement createTStmt = tddlCon.prepareStatement(CREATE_TABLE);
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

            for (String D : deleteString) {
                PreparedStatement p = tddlCon.prepareStatement(D);
                p.execute();
            }

            assert createFunctionStmt.size() == queryString.size();
            for (int i = 0; i < createFunctionStmt.size(); i++) {
                PreparedStatement p1 = tddlCon.prepareStatement(createFunctionStmt.get(i));
                p1.execute();
                PreparedStatement p2 = tddlCon.prepareStatement(queryString.get(i));
                ResultSet rs = p2.executeQuery();
                String r = null;
                while (rs.next()) {
                    r = rs.getObject(colIndex.get(i)).toString();
                }
                Assert.assertEquals(expectString.get(i), r);
            }
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    }
}
