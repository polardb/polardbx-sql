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

package com.alibaba.polardbx.qatest.dal.set;

import com.alibaba.druid.util.JdbcUtils;
import com.alibaba.polardbx.qatest.DirectConnectionBaseTestCase;
import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by wumu.stt on 20-07-13.
 */
public class SetCmdAssignmentTest extends DirectConnectionBaseTestCase {

    @Test
    public void testSocketTimeout() throws SQLException {
        String variable = "sockettimeout";

        String setSql = "set @@session." + variable + " = 0";
        String showSql = "show variables like '" + variable + "'";
        String setVarSql = "set @savedValue = @@session." + variable;
        String setTmpVarSql = "set @tmp = @@session." + variable;
        String selectSql = "select @savedValue";
        String selectTmpSql = "select @tmp";

        int oldValue = getIntValue(showSql, 2);
        try {
            //保存初始值到 变量@savedValue 以及 oldValue中
            setVar(setVarSql);
            Assert.assertEquals(getIntValue(selectSql, 1), oldValue);

            //测试简单set赋值
            setVar(setSql);
            Assert.assertEquals(0, getIntValue(showSql, 2));

            setSql = "set @@session." + variable + " = 1024";
            setVar(setSql);
            Assert.assertEquals(1024, getIntValue(showSql, 2));

            //测试set @a = @@b;
            setVar(setTmpVarSql);
            Assert.assertEquals(1024, getIntValue(selectTmpSql, 1));

            //set @@b = @a;
            setTmpVarSql = "set @tmp = 2048";
            setVar(setTmpVarSql);
            setSql = "set @@" + variable + " = @tmp";
            setVar(setSql);
            Assert.assertEquals(2048, getIntValue(showSql, 2));

            // Recover the oldValue   set @@b = @a;
            setSql = "set @@" + variable + " = @savedValue";
            setVar(setSql);
            Assert.assertEquals(getIntValue(showSql, 2), oldValue);
        } finally {
            // Recover the oldValue
            setSql = "set @@session." + variable + " = " + oldValue;
            setVar(setSql);
            Assert.assertEquals(getIntValue(showSql, 2), oldValue);
            this.tddlConnection.close();
        }
    }

    @Test
    public void testAsyncDDLPureMode() throws SQLException {
        String variable = "PURE_ASYNC_DDL_MODE";

        String setSql = "set @@session." + variable + " = 0";
        String showSql = "show variables like '" + variable + "'";
        String setVarSql = "set @savedValue = @@session." + variable;
        String setTmpVarSql = "set @tmp = @@session." + variable;
        String selectSql = "select @savedValue";
        String selectTmpSql = "select @tmp";

        try {
            //设置初始值
            setVar(setSql);
            Assert.assertTrue(getBooleanValue(showSql, 2) == false);

            //保存初始值到 变量@savedValue 以及 oldValue中
            setVar(setVarSql);
            boolean oldValue = getBooleanValue(showSql, 2);
            Assert.assertEquals(getBooleanValue(selectSql, 1), oldValue);

            setSql = "set @@session." + variable + " = 1";
            setVar(setSql);
            Assert.assertTrue(getBooleanValue(showSql, 2) == true);

            setSql = "set @@session." + variable + " = on";
            setVar(setSql);
            Assert.assertTrue(getBooleanValue(showSql, 2) == true);

            setSql = "set @@session." + variable + " = true";
            setVar(setSql);
            Assert.assertTrue(getBooleanValue(showSql, 2) == true);

            setSql = "set @@session." + variable + " = off";
            setVar(setSql);
            Assert.assertTrue(getBooleanValue(showSql, 2) == false);

            setSql = "set @@session." + variable + " = false";
            setVar(setSql);
            Assert.assertTrue(getBooleanValue(showSql, 2) == false);

            setSql = "set @@session." + variable + " = true";
            setVar(setSql);
            Assert.assertTrue(getBooleanValue(showSql, 2) == true);

            //测试set @a = @@b;
            setVar(setTmpVarSql);
            Assert.assertTrue(getBooleanValue(selectTmpSql, 1) == true);

            //set @@b = @a;
            setTmpVarSql = "set @tmp = true";
            setVar(setTmpVarSql);
            setSql = "set @@" + variable + " = @tmp";
            setVar(setSql);
            Assert.assertTrue(getBooleanValue(showSql, 2) == true);

            // Recover the oldValue   set @@b = @a;
            setSql = "set @@" + variable + " = @savedValue";
            setVar(setSql);
            Assert.assertEquals(getBooleanValue(showSql, 2), oldValue);
        } finally {
            this.tddlConnection.close();
        }
    }

    @Ignore
    public void testTransactionPolicy() throws SQLException {

        String variable = "TRANSACTION POLICY";

        String showSql = "show variables like '" + variable + "'";
        String setVarSql = "set @savedValue = @@`" + variable + "`";
        String setTmpVarSql = "set @tmp = @@`" + variable + "`";
        String selectSql = "select @savedValue";
        String selectTmpSql = "select @tmp";

        int oldValue = getIntValue(showSql, 2);
        try {
            //保存初始值到 变量@savedValue 以及 oldValue中
            setVar(setVarSql);
            Assert.assertEquals(getIntValue(selectSql, 1), oldValue);

            String setSql = "set @@`" + variable + "` = 5";
            setVar(setSql);
            Assert.assertEquals(5, getIntValue(showSql, 2));

            //测试set @a = @@b;
            setVar(setTmpVarSql);
            Assert.assertEquals(5, getIntValue(selectTmpSql, 1));

            //set @@b = @a;
            setTmpVarSql = "set @tmp = 6";
            setVar(setTmpVarSql);
            setSql = "set @@`" + variable + "` = @tmp";
            setVar(setSql);
            Assert.assertEquals(6, getIntValue(showSql, 2));

            // Recover the oldValue   set @@b = @a;
            setSql = "set @@`" + variable + "` = @savedValue";
            setVar(setSql);
            Assert.assertEquals(getIntValue(showSql, 2), oldValue);
        } finally {
            // Recover the oldValue
            String setSql = "set @@`" + variable + "` = " + oldValue;
            setVar(setSql);
            Assert.assertEquals(getIntValue(showSql, 2), oldValue);
            this.tddlConnection.close();
        }
    }

    @Test
    public void testTransPolicy() throws SQLException {

        String variable = "TRANS.POLICY";

        String showSql = "show variables like '" + variable + "'";
        String setVarSql = "set @savedValue = @@`" + variable + "`";
        String setTmpVarSql = "set @tmp = @@`" + variable + "`";
        String selectSql = "select @savedValue";
        String selectTmpSql = "select @tmp";
        String commitSql = "commit";

        String oldValue = getStringValue(showSql, 2);
        try {
            //关闭autocommit
            String setSql = "set @@autocommit = 0";
            setVar(setSql);

            //保存初始值到 变量@savedValue 以及 oldValue中
            setVar(setVarSql);
            Assert.assertEquals(getStringValue(selectSql, 1), oldValue);

            setSql = "set @@`" + variable + "` = 'BEST_EFFORT'";
            setVar(commitSql);
            setVar(setSql);
            Assert.assertEquals("BEST_EFFORT", getStringValue(showSql, 2));

            //测试set @a = @@b;
            setVar(setTmpVarSql);
            Assert.assertEquals("BEST_EFFORT", getStringValue(selectTmpSql, 1));

            //set @@b = @a;
            setTmpVarSql = "set @tmp = 'BEST_EFFORT'";
            setVar(setTmpVarSql);
            setSql = "set @@`" + variable + "` = @tmp";
            setVar(commitSql);
            setVar(setSql);
            Assert.assertEquals("BEST_EFFORT", getStringValue(showSql, 2));

            // Recover the oldValue   set @@b = @a;
            setSql = "set @@`" + variable + "` = @savedValue";
            setVar(commitSql);
            setVar(setSql);
            Assert.assertEquals(getStringValue(showSql, 2), oldValue);
        } finally {
            // Recover
            String setSql = "set @@`" + variable + "` = " + oldValue;
            setVar(setSql);
            Assert.assertEquals(getStringValue(showSql, 2), oldValue);
            setSql = "set @@autocommit = 1";
            setVar(setSql);
            this.tddlConnection.close();
        }
    }

    @Test
    public void testRead() throws SQLException {

        String variable = "READ";

        String showSql = "show variables like '" + variable + "'";
        String setVarSql = "set @savedValue = @@" + variable;
        String setTmpVarSql = "set @tmp = @@" + variable;
        String selectSql = "select @savedValue";
        String selectTmpSql = "select @tmp";

        String oldValue = getStringValue(showSql, 2);
        try {
            //保存初始值到 变量@savedValue 以及 oldValue中
            setVar(setVarSql);
            Assert.assertEquals(getStringValue(selectSql, 1), oldValue);

            String setSql = "set @@" + variable + " = 'WRITE'";
            setVar(setSql);
            Assert.assertEquals("WRITE", getStringValue(showSql, 2));

            //测试set @a = @@b;
            setVar(setTmpVarSql);
            Assert.assertEquals("WRITE", getStringValue(selectTmpSql, 1));

            // Recover the oldValue   set @@b = @a;
            setSql = "set @@" + variable + " = @savedValue";
            setVar(setSql);
            Assert.assertEquals(getStringValue(showSql, 2), oldValue);
        } finally {
            // Recover the oldValue
            String setSql = "set @@" + variable + " = '" + oldValue + "'";
            setVar(setSql);
            Assert.assertEquals(getStringValue(showSql, 2), oldValue);
            this.tddlConnection.close();
        }
    }

    @Test
    public void testBachInsertPolicy() throws SQLException {

        String variable = "BATCH_INSERT_POLICY";

        String showSql = "show variables like '" + variable + "'";
        String setVarSql = "set @savedValue = @@" + variable;
        String setTmpVarSql = "set @tmp = @@" + variable;
        String selectSql = "select @savedValue";
        String selectTmpSql = "select @tmp";

        String oldValue = getStringValue(showSql, 2);
        try {
            //保存初始值到 变量@savedValue 以及 oldValue中
            setVar(setVarSql);
            Assert.assertEquals(getStringValue(selectSql, 1), oldValue);

            String setSql = "set @@" + variable + " = 'SPLIT'";
            setVar(setSql);
            Assert.assertEquals("SPLIT", getStringValue(showSql, 2));

            setSql = "set @@" + variable + " = 'NONE'";
            setVar(setSql);
            Assert.assertEquals("NONE", getStringValue(showSql, 2));

            //测试set @a = @@b;
            setVar(setTmpVarSql);
            Assert.assertEquals("NONE", getStringValue(selectTmpSql, 1));

            // Recover the oldValue   set @@b = @a;
            setSql = "set @@" + variable + " = @savedValue";
            setVar(setSql);
            Assert.assertEquals(getStringValue(showSql, 2), oldValue);
        } finally {
            // Recover the oldValue
            String setSql = "set @@" + variable + " = '" + oldValue + "'";
            setVar(setSql);
            Assert.assertEquals(getStringValue(showSql, 2), oldValue);
            this.tddlConnection.close();
        }
    }

    @Test
    public void testAutoCommit() throws SQLException {

        String variable = "autocommit";

        String showSql = "show variables like '" + variable + "'";
        String setVarSql = "set @savedValue = @@session." + variable;
        String setTmpVarSql = "set @tmp = @@session." + variable;
        String selectTmpSql = "select @tmp";

        String oldValue = getStringValue(showSql, 2);
        try {
            //保存初始值到 变量@savedValue 以及 oldValue中
            setVar(setVarSql);

            String setSql = "set @@session." + variable + " = 0";
            setVar(setSql);
            Assert.assertEquals("OFF", getStringValue(showSql, 2));

            setSql = "set @@session." + variable + " = 1";
            setVar(setSql);
            Assert.assertEquals("ON", getStringValue(showSql, 2));

            setSql = "set @@session." + variable + " = on";
            setVar(setSql);
            Assert.assertEquals("ON", getStringValue(showSql, 2));

            setSql = "set @@session." + variable + " = true";
            setVar(setSql);
            Assert.assertEquals("ON", getStringValue(showSql, 2));

            setSql = "set @@session." + variable + " = off";
            setVar(setSql);
            Assert.assertEquals("OFF", getStringValue(showSql, 2));

            setSql = "set @@session." + variable + " = false";
            setVar(setSql);
            Assert.assertEquals("OFF", getStringValue(showSql, 2));

            setSql = "set @@session." + variable + " = true";
            setVar(setSql);
            Assert.assertEquals("ON", getStringValue(showSql, 2));

            //测试set @a = @@b;
            setVar(setTmpVarSql);
            Assert.assertEquals(true, getBooleanValue(selectTmpSql, 1));

            //set @@b = @a;
            setTmpVarSql = "set @tmp = true";
            setVar(setTmpVarSql);
            setSql = "set @@" + variable + " = @tmp";
            setVar(setSql);
            Assert.assertEquals("ON", getStringValue(showSql, 2));

            // Recover the oldValue   set @@b = @a;
            setSql = "set @@" + variable + " = @savedValue";
            setVar(setSql);
            Assert.assertEquals(getStringValue(showSql, 2), oldValue);
        } finally {
            // Recover the oldValue
            String setSql = "set @@" + variable + " = " + oldValue;
            setVar(setSql);
            Assert.assertEquals(getStringValue(showSql, 2), oldValue);
            this.tddlConnection.close();
        }
    }

    @Test
    public void testTxIsolation() throws SQLException {

        String variable = "TX_ISOLATION";

        String showSql = "show variables like '" + variable + "'";
        String setVarSql = "set @savedValue = @@" + variable;
        String setTmpVarSql = "set @tmp = @@" + variable;
        String selectSql = "select @savedValue";
        String selectTmpSql = "select @tmp";

        //如果初值为null 就设置一下初始值
        String setSql = "set @@" + variable + " = 'READ-COMMITTED'";
        setVar(setSql);
        String oldValue = getStringValue(showSql, 2);
        Assert.assertEquals("READ-COMMITTED", getStringValue(showSql, 2));
        try {
            //保存初始值到 变量@savedValue 以及 oldValue中
            setVar(setVarSql);
            Assert.assertEquals(getStringValue(selectSql, 1), oldValue);

            setSql = "set @@" + variable + " = 'READ-UNCOMMITTED'";
            setVar(setSql);
            Assert.assertEquals("READ-UNCOMMITTED", getStringValue(showSql, 2));

            //测试set @a = @@b;
            setVar(setTmpVarSql);
            Assert.assertEquals("READ-UNCOMMITTED", getStringValue(selectTmpSql, 1));

            //set @@b = @a;
            setTmpVarSql = "set @tmp = 'SERIALIZABLE'";
            setVar(setTmpVarSql);
            setSql = "set @@`" + variable + "` = @tmp";
            setVar(setSql);
            Assert.assertEquals("SERIALIZABLE", getStringValue(showSql, 2));

            // Recover the oldValue   set @@b = @a;
            setSql = "set @@" + variable + " = @savedValue";
            setVar(setSql);
            Assert.assertEquals(getStringValue(showSql, 2), oldValue);
        } finally {
            // Recover the oldValue
            setSql = "set @@" + variable + " = '" + oldValue + "'";
            setVar(setSql);
            Assert.assertEquals(getStringValue(showSql, 2), oldValue);
            this.tddlConnection.close();
        }
    }

    @Test
    public void testGroupConcatMaxLen() throws SQLException {

        String variable = "group_concat_max_len";

        String showSql = "show variables like '" + variable + "'";
        String setVarSql = "set @savedValue = @@session." + variable;
        String setTmpVarSql = "set @tmp = @@session." + variable;
        String selectSql = "select @savedValue";
        String selectTmpSql = "select @tmp";

        int oldValue = getIntValue(showSql, 2);
        try {
            //保存初始值到 变量@savedValue 以及 oldValue中
            setVar(setVarSql);
            Assert.assertEquals(getIntValue(selectSql, 1), oldValue);

            String setSql = "set @@session." + variable + " = 512";
            setVar(setSql);
            Assert.assertEquals(512, getIntValue(showSql, 2));

            //测试set @a = @@b;
            setVar(setTmpVarSql);
            Assert.assertEquals(512, getIntValue(selectTmpSql, 1));

            //set @@b = @a;
            setTmpVarSql = "set @tmp = 2048";
            setVar(setTmpVarSql);
            setSql = "set @@" + variable + " = @tmp";
            setVar(setSql);
            Assert.assertEquals(2048, getIntValue(showSql, 2));

            // Recover the oldValue   set @@b = @a;
            setSql = "set @@" + variable + " = @savedValue";
            setVar(setSql);
            Assert.assertEquals(getIntValue(showSql, 2), oldValue);
        } finally {
            // Recover the oldValue
            String setSql = "set @@" + variable + " = " + oldValue;
            setVar(setSql);
            Assert.assertEquals(getIntValue(showSql, 2), oldValue);
            this.tddlConnection.close();
        }
    }

    @Test
    @Ignore
    public void testCharacterSet() throws SQLException {

        String variable = "CHARACTER_SET_CONNECTION";

        String setSql = "set @@" + variable + " = 'utf8mb4'";
        String showSql = "show variables like '" + variable + "'";
        String setVarSql = "set @savedValue = @@" + variable;
        String setTmpVarSql = "set @tmp = @@" + variable;
        String selectSql = "select @savedValue";
        String selectTmpSql = "select @tmp";

        //设置初始值
        setVar(setSql);
        Assert.assertEquals("utf8mb4", getStringValue(showSql, 2));
        String oldValue = getStringValue(showSql, 2);
        try {
            //保存初始值到 变量@savedValue 以及 oldValue中
            setVar(setVarSql);
            String value = getStringValue(showSql, 2);
            Assert.assertEquals(getStringValue(selectSql, 1), value);

            setSql = "set @@" + variable + " = 'latin1'";
            setVar(setSql);
            Assert.assertEquals("latin1", getStringValue(showSql, 2));

            setSql = "set @@" + variable + " = 'ascii'";
            setVar(setSql);
            Assert.assertEquals("ascii", getStringValue(showSql, 2));

            //测试set @a = @@b;
            setVar(setTmpVarSql);
            Assert.assertEquals("ascii", getStringValue(selectTmpSql, 1));

            //set @@b = @a;
            setTmpVarSql = "set @tmp = 13";
            setVar(setTmpVarSql);
            setSql = "set @@`" + variable + "` = @tmp";
            setVar(setSql);
            Assert.assertEquals("sjis", getStringValue(showSql, 2));

            // Recover the oldValue   set @@b = @a;
            setSql = "set @@" + variable + " = @savedValue";
            setVar(setSql);
            Assert.assertEquals(getStringValue(showSql, 2), value);
        } finally {
            // Recover the oldValue
            setSql = "set @@" + variable + " = " + oldValue;
            setVar(setSql);
            Assert.assertEquals(getStringValue(showSql, 2), oldValue);
            this.tddlConnection.close();
        }
    }

    @Test
    public void testTimezone() throws SQLException {

        String variable = "TIME_ZONE";

        String setSql = "set @@" + variable + " = '+08:00'";
        String showSql = "show variables like '" + variable + "'";
        String setVarSql = "set @savedValue = @@" + variable;
        String setTmpVarSql = "set @tmp = @@" + variable;
        String selectSql = "select @savedValue";
        String selectTmpSql = "select @tmp";

        String oldValue = getStringValue(showSql, 2);
        try {
            //设置初始值
            setVar(setSql);
            Assert.assertEquals("+08:00", getStringValue(showSql, 2));

            //保存初始值到 变量@savedValue 以及 oldValue中
            setVar(setVarSql);
            String value = getStringValue(showSql, 2);
            Assert.assertEquals(getStringValue(selectSql, 1), value);

            setSql = "set @@" + variable + " = '+10:00'";
            setVar(setSql);
            Assert.assertEquals("+10:00", getStringValue(showSql, 2));

            setSql = "set @@" + variable + " = '-05:00'";
            setVar(setSql);
            Assert.assertEquals("-05:00", getStringValue(showSql, 2));

            //测试set @a = @@b;
            setVar(setTmpVarSql);
            Assert.assertEquals("-05:00", getStringValue(selectTmpSql, 1));

            //set @@b = @a;
            setTmpVarSql = "set @tmp = '+00:00'";
            setVar(setTmpVarSql);
            setSql = "set @@`" + variable + "` = @tmp";
            setVar(setSql);
            Assert.assertEquals("+00:00", getStringValue(showSql, 2));

            // Recover the oldValue   set @@b = @a;
            setSql = "set @@" + variable + " = @savedValue";
            setVar(setSql);
            Assert.assertEquals(getStringValue(showSql, 2), value);
        } finally {
            // Recover the oldValue
            setSql = "set @@" + variable + " = '" + oldValue + "'";
            setVar(setSql);
            Assert.assertEquals(getStringValue(showSql, 2), oldValue);
            this.tddlConnection.close();
        }
    }

    @Test
    @Ignore
    public void testSqlMode() throws SQLException {

        String variable = "sql_mode";

        String sqlMode = "STRICT_TRANS_TABLES,NO_AUTO_CREATE_USER,NO_ENGINE_SUBSTITUTION";
        if (isMySQL80()) {
            sqlMode = "STRICT_TRANS_TABLES,NO_ENGINE_SUBSTITUTION";
        }
        String setSql = "set @@" + variable + " = '" + sqlMode + "'";
        String showSql = "show variables like '" + variable + "'";
        String setVarSql = "set @savedValue = @@" + variable;
        String setTmpVarSql = "set @tmp = @@" + variable;
        String selectSql = "select @savedValue";
        String selectTmpSql = "select @tmp";

        //设置初始值
        setVar(setSql);
        String oldValue = getStringValue(showSql, 2);
        Assert.assertEquals(sqlMode, getStringValue(showSql, 2));
        try {
            //保存初始值到 变量@savedValue 以及 oldValue中
            setVar(setVarSql);
            String value = getStringValue(showSql, 2);
            Assert.assertEquals(getStringValue(selectSql, 1), value);

            setSql = "set @@" + variable + " = 'ONLY_FULL_GROUP_BY'";
            setVar(setSql);
            Assert.assertEquals("ONLY_FULL_GROUP_BY", getStringValue(showSql, 2));

            if (!isMySQL80()) {
                setSql = "set @@" + variable + " = 'NO_AUTO_CREATE_USER'";
                setVar(setSql);
                Assert.assertEquals("NO_AUTO_CREATE_USER", getStringValue(showSql, 2));

                //测试set @a = @@b;
                setVar(setTmpVarSql);
                Assert.assertEquals("NO_AUTO_CREATE_USER", getStringValue(selectTmpSql, 1));
            }

            //set @@b = @a;
            setTmpVarSql = "set @tmp = 'NO_ENGINE_SUBSTITUTION'";
            setVar(setTmpVarSql);
            setSql = "set @@`" + variable + "` = @tmp";
            setVar(setSql);
            Assert.assertEquals("NO_ENGINE_SUBSTITUTION", getStringValue(showSql, 2));

            // Recover the oldValue   set @@b = @a;
            setSql = "set @@" + variable + " = @savedValue";
            setVar(setSql);
            Assert.assertEquals(getStringValue(showSql, 2), value);
        } finally {
            // Recover the oldValue
            setSql = "set @@" + variable + " = '" + oldValue + "'";
            setVar(setSql);
            Assert.assertEquals(getStringValue(showSql, 2), oldValue);
            this.tddlConnection.close();
        }
    }

    @Test
    public void testUniqueChecks() throws SQLException {

        String variable = "unique_checks";

        String showSql = "show variables like '" + variable + "'";
        String setVarSql = "set @savedValue = @@session." + variable;
        String setTmpVarSql = "set @tmp = @@session." + variable;
        String selectTmpSql = "select @tmp";
        String selectSql = "select @savedValue";

        String oldValue = getStringValue(showSql, 2);
        try {
            //保存初始值到 变量@savedValue 以及 oldValue中
            setVar(setVarSql);
            Assert.assertEquals(getStringValue(selectSql, 1), oldValue);

            String setSql = "set @@session." + variable + " = 0";
            setVar(setSql);
            Assert.assertEquals("OFF", getStringValue(showSql, 2));

            setSql = "set @@session." + variable + " = 1";
            setVar(setSql);
            Assert.assertEquals("ON", getStringValue(showSql, 2));

            setSql = "set @@session." + variable + " = on";
            setVar(setSql);
            Assert.assertEquals("ON", getStringValue(showSql, 2));

            setSql = "set @@session." + variable + " = true";
            setVar(setSql);
            Assert.assertEquals("ON", getStringValue(showSql, 2));

            setSql = "set @@session." + variable + " = off";
            setVar(setSql);
            Assert.assertEquals("OFF", getStringValue(showSql, 2));

            setSql = "set @@session." + variable + " = false";
            setVar(setSql);
            Assert.assertEquals("OFF", getStringValue(showSql, 2));

            setSql = "set @@session." + variable + " = true";
            setVar(setSql);
            Assert.assertEquals("ON", getStringValue(showSql, 2));

            //测试set @a = @@b;
            setVar(setTmpVarSql);
            Assert.assertEquals(true, getBooleanValue(selectTmpSql, 1));

            //set @@b = @a;
            setTmpVarSql = "set @tmp = true";
            setVar(setTmpVarSql);
            setSql = "set @@" + variable + " = @tmp";
            setVar(setSql);
            Assert.assertEquals("ON", getStringValue(showSql, 2));

            // Recover the oldValue   set @@b = @a;
            setSql = "set @@" + variable + " = @savedValue";
            setVar(setSql);
            Assert.assertEquals(getStringValue(showSql, 2), oldValue);
        } finally {
            // Recover the oldValue
            String setSql = "set @@" + variable + " = " + oldValue;
            setVar(setSql);
            Assert.assertEquals(getStringValue(showSql, 2), oldValue);
            this.tddlConnection.close();
        }
    }

    @Test
    public void testReadBufferSize() throws SQLException {

        String variable = "read_buffer_size";

        String showSql = "show variables like '" + variable + "'";
        String setVarSql = "set @savedValue = @@session." + variable;
        String setTmpVarSql = "set @tmp = @@session." + variable;
        String selectSql = "select @savedValue";
        String selectTmpSql = "select @tmp";

        int oldValue = getIntValue(showSql, 2);
        try {
            //保存初始值到 变量@savedValue 以及 oldValue中
            setVar(setVarSql);

            Assert.assertEquals(getIntValue(selectSql, 1), oldValue);

            String setSql = "set @@session." + variable + " = 65536";
            setVar(setSql);
            Assert.assertEquals(65536, getIntValue(showSql, 2));

            //测试set @a = @@b;
            setVar(setTmpVarSql);
            Assert.assertEquals(65536, getIntValue(selectTmpSql, 1));

            //set @@b = @a;
            setTmpVarSql = "set @tmp = 1048576";
            setVar(setTmpVarSql);
            setSql = "set @@" + variable + " = @tmp";
            setVar(setSql);
            Assert.assertEquals(1048576, getIntValue(showSql, 2));

            // Recover the oldValue   set @@b = @a;
            setSql = "set @@" + variable + " = @savedValue";
            setVar(setSql);
            Assert.assertEquals(getIntValue(showSql, 2), oldValue);
        } finally {
            // Recover the oldValue
            String setSql = "set @@" + variable + " = " + oldValue;
            setVar(setSql);
            Assert.assertEquals(getIntValue(showSql, 2), oldValue);
            this.tddlConnection.close();
        }
    }

    @Test
    public void testOptimizerTrace() throws SQLException {

        String variable = "optimizer_trace";

        String showSql = "show variables like '" + variable + "'";
        String setVarSql = "set @savedValue = @@" + variable;
        String setTmpVarSql = "set @tmp = @@" + variable;
        String selectSql = "select @savedValue";
        String selectTmpSql = "select @tmp";

        String oldValue = getStringValue(showSql, 2);
        try {
            //保存初始值到 变量@savedValue 以及 oldValue中
            setVar(setVarSql);
            Assert.assertEquals(getStringValue(selectSql, 1), oldValue);

            String setSql = "set @@" + variable + " = 'enabled=on,one_line=off'";
            setVar(setSql);
            Assert.assertEquals("enabled=on,one_line=off", getStringValue(showSql, 2));

            //测试set @a = @@b;
            setVar(setTmpVarSql);
            Assert.assertEquals("enabled=on,one_line=off", getStringValue(selectTmpSql, 1));

            //set @@b = @a;
            setTmpVarSql = "set @tmp = 'enabled=on,one_line=on'";
            setVar(setTmpVarSql);
            setSql = "set @@`" + variable + "` = @tmp";
            setVar(setSql);
            Assert.assertEquals("enabled=on,one_line=on", getStringValue(showSql, 2));

            // Recover the oldValue   set @@b = @a;
            setSql = "set @@" + variable + " = @savedValue";
            setVar(setSql);
            Assert.assertEquals(getStringValue(showSql, 2), oldValue);
        } finally {
            // Recover the oldValue
            String setSql = "set @@" + variable + " = '" + oldValue + "'";
            setVar(setSql);
            Assert.assertEquals(getStringValue(showSql, 2), oldValue);
            this.tddlConnection.close();
        }
    }

    private int getIntValue(String sql, int index) {
        int value = -1;
        ResultSet rs = null;
        Statement statement = null;
        try {
            statement = this.tddlConnection.createStatement();
            rs = statement.executeQuery(sql);
            while (rs.next()) {
                value = rs.getInt(index);
            }
        } catch (SQLException e) {
        } finally {
            JdbcUtils.close(rs);
            JdbcUtils.close(statement);
        }
        return value;
    }

    private String getStringValue(String sql, int index) {
        String value = null;
        ResultSet rs = null;
        Statement statement = null;
        try {
            statement = this.tddlConnection.createStatement();
            rs = statement.executeQuery(sql);
            while (rs.next()) {
                value = StringUtils.strip(rs.getString(index), " ");
            }
        } catch (SQLException e) {
        } finally {
            JdbcUtils.close(rs);
            JdbcUtils.close(statement);
        }
        return value;
    }

    private boolean getBooleanValue(String sql, int index) {
        boolean value = false;
        ResultSet rs = null;
        Statement statement = null;
        try {
            statement = this.tddlConnection.createStatement();
            rs = statement.executeQuery(sql);
            while (rs.next()) {
                value = rs.getBoolean(index);
            }
        } catch (SQLException e) {
        } finally {
            JdbcUtils.close(rs);
            JdbcUtils.close(statement);
        }
        return value;
    }

    private void setVar(String sql) throws SQLException {
        Statement statement = null;
        try {
            statement = this.tddlConnection.createStatement();
            statement.execute(sql);
        } finally {
            JdbcUtils.close(statement);
        }
    }
}
