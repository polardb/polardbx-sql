package com.alibaba.polardbx.server.parser;

import com.alibaba.polardbx.druid.sql.parser.ByteString;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static org.junit.Assert.assertEquals;

public class ServerParseShowSqlEngineAlertCheckTest {

    private int sqlEngineAlertCheck(ByteString stmt, int offset) {
        try {
            ServerParseShow yourClass = new ServerParseShow();
            Method privateMethod =
                ServerParseShow.class.getDeclaredMethod("sqlEngineAlertCheck", ByteString.class, int.class);
            // 设置可访问性为 true
            privateMethod.setAccessible(true);
            return (int) privateMethod.invoke(yourClass, stmt, offset);
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    private int fullCheck(ByteString stmt, int offset) {
        try {
            ServerParseShow yourClass = new ServerParseShow();
            Method privateMethod =
                ServerParseShow.class.getDeclaredMethod("fullCheck", ByteString.class, int.class);
            // 设置可访问性为 true
            privateMethod.setAccessible(true);
            return (int) privateMethod.invoke(yourClass, stmt, offset);
        } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 测试完整匹配 "ql_engine alert"
     */
    @Test
    public void testFullMatchQL_ENGINE_ALERT() {
        ByteString stmt = ByteString.from("sql_engine alert");
        assertEquals(ServerParseShow.SQL_ENGINE_ALERT, sqlEngineAlertCheck(stmt, 0));
    }

    /**
     * 测试只匹配前半部分
     */
    @Test
    public void testPartialMatch() {
        ByteString stmt = ByteString.from("sql_engine");
        assertEquals(ServerParseShow.OTHER, sqlEngineAlertCheck(stmt, 0));
    }

    /**
     * 测试不完整匹配
     */
    @Test
    public void testIncompleteMatch() {
        ByteString stmt = ByteString.from("sql_engin");
        assertEquals(ServerParseShow.OTHER, sqlEngineAlertCheck(stmt, 0));
    }

    /**
     * 测试在字符串中间匹配
     */
    @Test
    public void testMatchInMiddle() {
        ByteString stmt = ByteString.from("some sql_engine alert");
        assertEquals(ServerParseShow.SQL_ENGINE_ALERT, sqlEngineAlertCheck(stmt, 5));
    }

    /**
     * 测试中间有下划线不匹配
     */
    @Test
    public void testWithUnderscore() {
        ByteString stmt = ByteString.from("sql_engine_alert");
        assertEquals(ServerParseShow.OTHER, sqlEngineAlertCheck(stmt, 0));
    }

    /**
     * 测试大写形式匹配
     */
    @Test
    public void testUpperCaseMatch() {
        ByteString stmt = ByteString.from("SQL_ENGINE ALERT");
        assertEquals(ServerParseShow.SQL_ENGINE_ALERT, sqlEngineAlertCheck(stmt, 0));
    }

    /**
     * 测试中间缺少空格不匹配
     */
    @Test
    public void testWithoutSpace() {
        ByteString stmt = ByteString.from("sql_engineaalert");
        assertEquals(ServerParseShow.OTHER, sqlEngineAlertCheck(stmt, 0));
    }

    /**
     * 测试使用制表符分隔匹配
     */
    @Test
    public void testWithTabSeparator() {
        ByteString stmt = ByteString.from("sql_engine\talert");
        assertEquals(ServerParseShow.SQL_ENGINE_ALERT, sqlEngineAlertCheck(stmt, 0));
    }

    /**
     * 测试使用换行符分隔匹配
     */
    @Test
    public void testWithNewlineSeparator() {
        ByteString stmt = ByteString.from("sql_engine\nalert");
        assertEquals(ServerParseShow.SQL_ENGINE_ALERT, sqlEngineAlertCheck(stmt, 0));
    }

    /**
     * 测试使用回车符分隔匹配
     */
    @Test
    public void testWithCarriageReturnSeparator() {
        ByteString stmt = ByteString.from("sql_engine\ralert");
        assertEquals(ServerParseShow.SQL_ENGINE_ALERT, sqlEngineAlertCheck(stmt, 0));
    }

    /**
     * 测试使用多个空格分隔匹配
     */
    @Test
    public void testWithMultipleSpacesSeparator() {
        ByteString stmt = ByteString.from("sql_engine  alert");
        assertEquals(ServerParseShow.SQL_ENGINE_ALERT, sqlEngineAlertCheck(stmt, 0));
    }

    @Test
    public void testFullWithTabSeparatorFail() {
        ByteString stmt = ByteString.from("full sql_engines alert");
        assertEquals(ServerParseShow.OTHER, fullCheck(stmt, 0));
    }
}
