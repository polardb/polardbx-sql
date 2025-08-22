package com.alibaba.polardbx.optimizer.core.planner;

import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.apache.calcite.sql.OutFileParams;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.TDDLSqlSelect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.lang.reflect.Method;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PlannerEnableRecordViewMapTest {

    private Planner testPlanner;
    private PlannerContext mockPlannerContext;
    private ExecutionContext executionContext;

    @Before
    public void setUp() {
        testPlanner = new Planner();
        mockPlannerContext = mock(PlannerContext.class);
        executionContext = new ExecutionContext();
    }

    @After
    public void tearDown() {
        mockPlannerContext = null;
        executionContext = null;
        testPlanner = null;
    }

    private void enableRecordViewMap(SqlNode ast, PlannerContext plannerContext) {
        Method method;
        try {
            method = Planner.class.getDeclaredMethod("enableRecordViewMap", SqlNode.class, PlannerContext.class);
            method.setAccessible(true);
            method.invoke(testPlanner, ast, plannerContext);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * TC01: ast 是 TDDLSqlSelect 实例，outFileParams 不为 null，IsStatistics 返回 true
     */
    @Test
    public void test_enableRecordViewMap_with_TDDLSqlSelect_IsStatistics_true() {
        try (MockedStatic<OutFileParams> mockedOutFile = mockStatic(OutFileParams.class)) {
            // Arrange
            TDDLSqlSelect mockSelect = mock(TDDLSqlSelect.class);
            OutFileParams mockOutFileParams = mock(OutFileParams.class);
            when(mockSelect.getOutFileParams()).thenReturn(mockOutFileParams);
            mockedOutFile.when(() -> OutFileParams.IsStatistics(mockOutFileParams)).thenReturn(true);

            // Act
            enableRecordViewMap(mockSelect, mockPlannerContext);

            // Assert
            verify(mockPlannerContext, times(1)).setEnableSelectStatistics();
        }
    }

    /**
     * TC02: ast 是 TDDLSqlSelect 实例，但 outFileParams 为 null
     */
    @Test
    public void test_enableRecordViewMap_with_TDDLSqlSelect_OutFileParams_null() {
        // Arrange
        TDDLSqlSelect mockSelect = mock(TDDLSqlSelect.class);
        when(mockSelect.getOutFileParams()).thenReturn(null);

        // Act
        enableRecordViewMap(mockSelect, mockPlannerContext);

        // Assert
        verify(mockPlannerContext, never()).setEnableSelectStatistics();
    }

    /**
     * TC03: ast 是 SqlWith 实例，body 是 TDDLSqlSelect，outFileParams 不为 null，IsStatistics 返回 false
     */
    @Test
    public void test_enableRecordViewMap_with_SqlWith_TDDLSqlSelect_IsStatistics_false() {
        try (MockedStatic<OutFileParams> mockedOutFile = mockStatic(OutFileParams.class)) {
            // Arrange
            TDDLSqlSelect mockSelect = mock(TDDLSqlSelect.class);
            OutFileParams mockOutFileParams = mock(OutFileParams.class);
            SqlWith with = new SqlWith(SqlParserPos.ZERO, mock(SqlNodeList.class), mockSelect);
            when(mockSelect.getOutFileParams()).thenReturn(mockOutFileParams);
            mockedOutFile.when(() -> OutFileParams.IsStatistics(mockOutFileParams)).thenReturn(true);

            // Act
            enableRecordViewMap(with, mockPlannerContext);

            // Assert
            verify(mockPlannerContext, times(1)).setEnableSelectStatistics();
        }
    }

    /**
     * TC04: ast 是 SqlWith 实例，但 body 不是 TDDLSqlSelect
     */
    @Test
    public void test_enableRecordViewMap_with_SqlWith_Non_TDDLSqlSelect_body() {
        // Arrange
        SqlWith with = new SqlWith(SqlParserPos.ZERO, mock(SqlNodeList.class), null);

        // Act
        enableRecordViewMap(with, mockPlannerContext);

        // Assert
        verify(mockPlannerContext, never()).setEnableSelectStatistics();
    }

    /**
     * TC05: ast 是其他类型（如 SqlInsert）
     */
    @Test
    public void test_enableRecordViewMap_with_other_sql_node_type() {
        // Arrange
        SqlNode mockInsert = mock(SqlNode.class);

        // Act
        enableRecordViewMap(mockInsert, mockPlannerContext);

        // Assert
        verify(mockPlannerContext, never()).setEnableSelectStatistics();
    }
}
