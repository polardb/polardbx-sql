package com.alibaba.polardbx.druid.sql.visitor;

import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLBinaryExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLBooleanExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLDateExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLHexExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLListExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLNCharExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLNullExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLTimeExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLTimestampExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLVariantRefExpr;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class ExportParameterVisitorUtilsTest {

    @Test
    public void testExportParameterWithSQLCharExpr() {
        List<Object> parameters = new ArrayList<>();
        SQLCharExpr expr = new SQLCharExpr("test");
        SQLExpr result = ExportParameterVisitorUtils.exportParameter(parameters, expr);
        assertNotNull(result);
        assertTrue(result instanceof SQLVariantRefExpr);
        assertEquals("?", ((SQLVariantRefExpr) result).getName());
        assertEquals("test", parameters.get(parameters.size() - 1));
    }

    @Test
    public void testExportParameterWithSQLListExpr() {
        List<Object> parameters = new ArrayList<>();
        SQLListExpr listExpr = new SQLListExpr();
        listExpr.getItems().add(new SQLCharExpr("value1"));
        listExpr.getItems().add(new SQLNCharExpr("value2"));
        listExpr.getItems().add(new SQLBooleanExpr(true));
        listExpr.getItems().add(new SQLIntegerExpr(123));
        listExpr.getItems().add(new SQLHexExpr("123"));
        listExpr.getItems().add(new SQLBinaryExpr("010"));
        listExpr.getItems().add(new SQLTimestampExpr("2023-07-01 12:34:56"));
        listExpr.getItems().add(new SQLDateExpr("2023-07-01"));
        listExpr.getItems().add(new SQLTimeExpr("12:34:56"));
        listExpr.getItems().add(new SQLNullExpr());

        SQLExpr result = ExportParameterVisitorUtils.exportParameter(parameters, listExpr);
        assertNotNull(result);
        assertTrue(result instanceof SQLVariantRefExpr);
        assertEquals("?", ((SQLVariantRefExpr) result).getName());
        assertTrue(parameters.get(0) instanceof List);

        List<Object> paramList = (List<Object>) parameters.get(0);

        assertEquals(10, paramList.size());
        assertEquals("value1", paramList.get(0));
        assertEquals("value2", paramList.get(1));
        assertEquals(true, paramList.get(2));
        assertEquals(123, paramList.get(3));
        assertTrue(Arrays.equals(new byte[] {1, 35}, (byte[]) paramList.get(4)));
        assertEquals(2L, paramList.get(5));
        assertEquals("2023-07-01 12:34:56", paramList.get(6));
        assertEquals("2023-07-01", paramList.get(7));
        assertEquals("12:34:56", paramList.get(8));
        assertNull(paramList.get(9));
    }
}