package com.alibaba.polardbx.repo.mysql.handler;

import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.common.privilege.MySQLPrivilegesName;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.subhandler.InformationSchemaCollationsHandler;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.table.ColumnsInfoSchemaRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dal.BaseDalOperation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlShow;
import org.junit.Test;
import org.mockito.MockedStatic;

import javax.sql.DataSource;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class MyBaseDalHandlerTest {

    @Test
    public void testHandleShowCollationInColumnarMode()
        throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        MyBaseDalHandler myBaseDalHandler = mock(MyBaseDalHandler.class);

        String schemaName = "test_schema";

        BaseDalOperation logicalPlan = mock(BaseDalOperation.class);
        ExecutionContext ec = mock(ExecutionContext.class);
        when(logicalPlan.getSchemaName()).thenReturn(schemaName);

        //verify
        Method method = myBaseDalHandler.getClass()
            .getDeclaredMethod("handleShowCollationInColumnarMode", RelNode.class, ExecutionContext.class);
        method.setAccessible(true);
        Cursor result = (Cursor) method.invoke(myBaseDalHandler, logicalPlan, ec);
        Assert.assertTrue(result instanceof ArrayResultCursor
            && ((ArrayResultCursor) result).getRows().size() == InformationSchemaCollationsHandler.COLLATIONS.length);
    }

    @Test
    public void testHandleShowCharacterSetInColumnarMode()
        throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        MyBaseDalHandler myBaseDalHandler = mock(MyBaseDalHandler.class);

        String schemaName = "test_schema";

        BaseDalOperation logicalPlan = mock(BaseDalOperation.class);
        ExecutionContext ec = mock(ExecutionContext.class);
        when(logicalPlan.getSchemaName()).thenReturn(schemaName);

        //verify
        Method method = myBaseDalHandler.getClass()
            .getDeclaredMethod("handleShowCharacterSetInColumnarMode", RelNode.class, ExecutionContext.class);
        method.setAccessible(true);
        Cursor result = (Cursor) method.invoke(myBaseDalHandler, logicalPlan, ec);
        Assert.assertTrue(result instanceof ArrayResultCursor
            && ((ArrayResultCursor) result).getRows().size() == CharsetName.values().length);
    }

    @Test
    public void testHandleShowPrivilegesInColumnarMode()
        throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        MyBaseDalHandler myBaseDalHandler = mock(MyBaseDalHandler.class);

        String schemaName = "test_schema";

        BaseDalOperation logicalPlan = mock(BaseDalOperation.class);
        ExecutionContext ec = mock(ExecutionContext.class);
        when(logicalPlan.getSchemaName()).thenReturn(schemaName);

        //verify
        Method method = myBaseDalHandler.getClass()
            .getDeclaredMethod("handleShowPrivilegesInColumnarMode", RelNode.class, ExecutionContext.class);
        method.setAccessible(true);
        Cursor result = (Cursor) method.invoke(myBaseDalHandler, logicalPlan, ec);
        Assert.assertTrue(result instanceof ArrayResultCursor
            && ((ArrayResultCursor) result).getRows().size() == MySQLPrivilegesName.values().length);
    }

    @Test
    public void testHandleShowColumnInColumnarMode()
        throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        String schemaName = "test_schema";
        String tableName = "test_table";
        MyBaseDalHandler myBaseDalHandler = mock(MyBaseDalHandler.class);

        BaseDalOperation logicalPlan = mock(BaseDalOperation.class);
        ExecutionContext ec = mock(ExecutionContext.class);

        //mock extractSchemaTableNameForShowColumns
        try (MockedStatic<ConfigDataMode> mockedConfigDataMode = mockStatic(ConfigDataMode.class)) {
            mockedConfigDataMode.when(() -> ConfigDataMode.isPolarDbX()).thenReturn(true);
            when(logicalPlan.getKind()).thenReturn(SqlKind.SHOW);
            SqlShow sqlShow = mock(SqlShow.class);
            when(logicalPlan.getNativeSqlNode()).thenReturn(sqlShow);
            when(logicalPlan.getNativeSql()).thenReturn("SHOW FULL COLUMNS");
            when(logicalPlan.getSchemaName()).thenReturn(schemaName);
            SqlIdentifier sqlIdentifier = mock(SqlIdentifier.class);
            when(sqlShow.getTableName()).thenReturn(sqlIdentifier);
            when(sqlIdentifier.getLastName()).thenReturn(tableName);

            //mock DataSourcek
            DataSource mockDataSource = mock(DataSource.class);
            MetaDbDataSource metaDbDataSource = mock(MetaDbDataSource.class);
            try (MockedStatic<MetaDbDataSource> mockedMetaDBDataSource = mockStatic(MetaDbDataSource.class)) {
                mockedMetaDBDataSource.when(() -> MetaDbDataSource.getInstance()).thenReturn(metaDbDataSource);
                when(metaDbDataSource.getDataSource()).thenReturn(mockDataSource);
                //mock Table MetaDB Utils
                try (MockedStatic<MetaDbUtil> mockedMetaDbUtil = mockStatic(MetaDbUtil.class)) {
                    List<ColumnsInfoSchemaRecord> tables = generateColumnsInfoSchemaRecords();
                    mockedMetaDbUtil.when(() -> MetaDbUtil.query(anyString(), any(), any(), any()))
                        .thenReturn(tables);
                    //verify
                    Method method = myBaseDalHandler.getClass()
                        .getDeclaredMethod("handleShowColumnInColumnarMode", RelNode.class, ExecutionContext.class);
                    method.setAccessible(true);
                    Cursor result = (Cursor) method.invoke(myBaseDalHandler, logicalPlan, ec);
                    Assert.assertTrue(
                        result instanceof ArrayResultCursor && ((ArrayResultCursor) result).getRows().size() == 2);
                }
            }
        }
    }

    List<ColumnsInfoSchemaRecord> generateColumnsInfoSchemaRecords() {
        ColumnsInfoSchemaRecord record1 = new ColumnsInfoSchemaRecord();
        record1.tableSchema = "test_schema1";
        record1.tableName = "test_table1";
        record1.columnName = "test_column1";
        record1.ordinalPosition = 1L;
        record1.columnDefault = "DEFAULT_VALUE1";
        record1.isNullable = "NO";
        record1.dataType = "INT";
        record1.characterMaximumLength = 0L;
        record1.characterOctetLength = 0L;
        record1.numericPrecision = 11L;
        record1.numericScale = 0L;
        record1.numericScaleNull = false;
        record1.datetimePrecision = 0L;
        record1.characterSetName = "utf8";
        record1.collationName = "utf8_general_ci";
        record1.columnType = "INT(11)";
        record1.columnKey = "";
        record1.extra = "";
        record1.privileges = "SELECT";
        record1.columnComment = "First Test Column";
        record1.generationExpression = "";

        ColumnsInfoSchemaRecord record2 = new ColumnsInfoSchemaRecord();
        record2.tableSchema = "test_schema2";
        record2.tableName = "test_table2";
        record2.columnName = "test_column2";
        record2.ordinalPosition = 2L;
        record2.columnDefault = null;
        record2.isNullable = "YES";
        record2.dataType = "VARCHAR";
        record2.characterMaximumLength = 255L;
        record2.characterOctetLength = 765L;
        record2.numericPrecision = 0L;
        record2.numericScale = 0L;
        record2.numericScaleNull = true;
        record2.datetimePrecision = 6L;
        record2.characterSetName = "latin1";
        record2.collationName = "latin1_swedish_ci";
        record2.columnType = "VARCHAR(255)";
        record2.columnKey = "MUL";
        record2.extra = "on update CURRENT_TIMESTAMP";
        record2.privileges = "SELECT,INSERT,UPDATE";
        record2.columnComment = "Second Test Column";
        record2.generationExpression = "CURRENT_TIMESTAMP()";
        return new ArrayList<>(Arrays.asList(record1, record2));
    }
}
