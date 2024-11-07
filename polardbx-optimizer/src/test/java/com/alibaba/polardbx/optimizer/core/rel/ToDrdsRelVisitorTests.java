package com.alibaba.polardbx.optimizer.core.rel;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.gms.metadb.table.IndexVisibility;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.IndexMeta;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.clearspring.analytics.util.Lists;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalTableLookup;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIndexHint;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;

import static com.alibaba.polardbx.common.utils.Assert.assertNotNull;
import static org.apache.calcite.sql.parser.SqlParserPos.ZERO;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ToDrdsRelVisitorTests {
    private static final String schemaName = "testSchema";

    /**
     * Tests that null is returned when the index node of the scan is not an instance of SqlNodeList
     */
    @Test
    public void testGetForceIndexWithNonSqlNodeListNode() {
        TableScan mockTableScan = mock(TableScan.class);

        when(mockTableScan.getIndexNode()).thenReturn(mock(SqlNode.class));

        assertNull(new ToDrdsRelVisitor().getForceIndex(mockTableScan));
    }

    /**
     * Tests that null is returned when the index node's list is empty
     */
    @Test
    public void testGetForceIndexWithEmptySqlNodeList() {
        TableScan mockTableScan = mock(TableScan.class);
        SqlNodeList mockSqlNodeList = mock(SqlNodeList.class);

        when(mockTableScan.getIndexNode()).thenReturn(mockSqlNodeList);
        when(mockSqlNodeList.getList()).thenReturn(Collections.emptyList());

        assertNull(new ToDrdsRelVisitor().getForceIndex(mockTableScan));
    }

    /**
     * Tests that null is returned when the first element in the index node's list is not a SqlIndexHint
     */
    @Test
    public void testGetForceIndexWithoutSqlIndexHint() {
        TableScan mockTableScan = mock(TableScan.class);
        SqlNodeList mockSqlNodeList = mock(SqlNodeList.class);

        when(mockTableScan.getIndexNode()).thenReturn(mockSqlNodeList);
        when(mockSqlNodeList.getList()).thenReturn(Collections.singletonList(mock(SqlNode.class)));

        assertNull(new ToDrdsRelVisitor().getForceIndex(mockTableScan));
    }

    /**
     * Tests that null is returned when the SqlIndexHint is null
     */
    @Test
    public void testGetForceIndexWithNullSqlIndexHint() {
        TableScan mockTableScan = mock(TableScan.class);
        SqlNodeList mockSqlNodeList = mock(SqlNodeList.class);

        when(mockTableScan.getIndexNode()).thenReturn(mockSqlNodeList);
        when(mockSqlNodeList.getList()).thenReturn(null);

        assertNull(new ToDrdsRelVisitor().getForceIndex(mockTableScan));
    }

    /**
     * Tests that null is returned when the SqlIndexHint has a null index list
     */
    @Test
    public void testGetForceIndexWithNullIndexListInSqlIndexHint() {
        TableScan mockTableScan = mock(TableScan.class);
        SqlNodeList mockSqlNodeList = mock(SqlNodeList.class);
        SqlIndexHint mockSqlIndexHint = mock(SqlIndexHint.class);

        when(mockTableScan.getIndexNode()).thenReturn(mockSqlNodeList);
        when(mockSqlNodeList.getList()).thenReturn(Collections.singletonList(mockSqlIndexHint));
        when(mockSqlIndexHint.getIndexList()).thenReturn(null);

        assertNull(new ToDrdsRelVisitor().getForceIndex(mockTableScan));
    }

    // test no force index
    @Test
    public void testBuildForceIndexWithoutForceIndex() {
        Engine engine = mock(Engine.class);
        RelOptSchema catalog = mock(RelOptSchema.class);
        TableMeta tMeta = mock(TableMeta.class);
        LogicalTableScan scan = mock(LogicalTableScan.class);
        ToDrdsRelVisitor visitor = mock(ToDrdsRelVisitor.class);

        when(scan.getIndexNode()).thenReturn(null);

        RelNode result = visitor.buildForceIndexByForceIndex(catalog, scan, schemaName, tMeta, engine);

        assertNull(result);
    }

    // test force index cci
    @Test
    public void testBuildForceIndexWithCci() {
        try (MockedStatic<LogicalTableScan> scanMockedStatic = mockStatic(LogicalTableScan.class);
            MockedConstruction<OSSTableScan> mockedConstruction = mockConstruction(OSSTableScan.class);
            MockedStatic<PlannerContext> plannerContextMockedStatic = mockStatic(PlannerContext.class)) {
            scanMockedStatic.when(() -> LogicalTableScan.create(any(), any(), any(), any(), any(), any(), any()))
                .thenReturn(mock(LogicalTableScan.class));
            plannerContextMockedStatic.when(() -> PlannerContext.getPlannerContext(any(RelNode.class)))
                .thenReturn(mock(PlannerContext.class));
            Engine engine = mock(Engine.class);
            RelOptSchema catalog = mock(RelOptSchema.class);
            LogicalTableScan scan = mock(LogicalTableScan.class);
            TableMeta tMeta = mock(TableMeta.class);
            GsiMetaManager.GsiIndexMetaBean cciMeta =
                new GsiMetaManager.GsiIndexMetaBean(
                    "testIndex",
                    "testSchema",
                    "testTable",
                    false,
                    "index_schema",
                    "cci",
                    Lists.newArrayList(),
                    Lists.newArrayList(),
                    "indexType",
                    "",
                    "",
                    null,
                    "indexTableName",
                    IndexStatus.PUBLIC,
                    0,
                    true,
                    true,
                    IndexVisibility.VISIBLE);
            ToDrdsRelVisitor visitor = mock(ToDrdsRelVisitor.class);

            when(scan.getIndexNode()).thenReturn(null);
            when(tMeta.findGlobalSecondaryIndexByName("cci")).thenReturn(cciMeta);
            when(visitor.getForceIndex(Mockito.any())).thenReturn(
                new SqlIdentifier(Collections.singletonList("cci"), ZERO));
            when(visitor.buildForceIndexByForceIndex(catalog, scan, schemaName, tMeta, engine)).thenCallRealMethod();
            when(
                visitor.buildForceIndex(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.any(), Mockito.any(),
                    Mockito.anyString()))
                .thenCallRealMethod();
            when(visitor.buildOSSTableScan(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.any(),
                Mockito.any()))
                .thenCallRealMethod();

            RelNode result = visitor.buildForceIndexByForceIndex(catalog, scan, schemaName, tMeta, engine);

            assertTrue(result instanceof OSSTableScan);
        }
    }

    // test force index with two names, gis exists and local index exists
    @Test
    public void testBuildForceIndexWithTwoNamesGsiLocalIndexExists() {
        Engine engine = mock(Engine.class);
        RelOptSchema catalog = mock(RelOptSchema.class);
        LogicalTableScan scan = mock(LogicalTableScan.class);
        TableMeta tMeta = mock(TableMeta.class);
        GsiMetaManager.GsiIndexMetaBean gsi = mock(GsiMetaManager.GsiIndexMetaBean.class);
        TableMeta gsiMeta = mock(TableMeta.class);
        ToDrdsRelVisitor visitor = mock(ToDrdsRelVisitor.class);
        LogicalTableLookup target = mock(LogicalTableLookup.class);
        PlannerContext plannerContext = mock(PlannerContext.class);
        ExecutionContext executionContext = mock(ExecutionContext.class);
        SchemaManager schemaManager = mock(SchemaManager.class);

        when(scan.getIndexNode()).thenReturn(null);
        when(tMeta.findGlobalSecondaryIndexByName("tablePart")).thenReturn(gsi);
        when(visitor.getForceIndex(scan)).thenReturn(new SqlIdentifier(Arrays.asList("tablePart", "indexPart"), ZERO));
        when(visitor.buildForceIndexByForceIndex(catalog, scan, schemaName, tMeta, engine)).thenCallRealMethod();
        when(visitor.getPlannerContext()).thenReturn(plannerContext);
        when(plannerContext.getExecutionContext()).thenReturn(executionContext);
        when(executionContext.getSchemaManager(schemaName)).thenReturn(schemaManager);
        when(schemaManager.getTable(gsi.indexTableName)).thenReturn(gsiMeta);
        when(tMeta.findLocalIndexByName("tablePart")).thenReturn(null);
        when(gsiMeta.findLocalIndexByName("indexPart")).thenReturn(mock(IndexMeta.class));
        when(visitor.buildLogicalTableLookup(catalog, scan, schemaName, engine, gsi, "indexPart")).thenReturn(target);

        RelNode result = visitor.buildForceIndexByForceIndex(catalog, scan, schemaName, tMeta, engine);

        assertEquals(target, result);
    }

    // test gsi not exists
    @Test
    public void testBuildForceIndexWithErrorTablePartNotExist() {
        Engine engine = mock(Engine.class);
        RelOptSchema catalog = mock(RelOptSchema.class);
        LogicalTableScan scan = mock(LogicalTableScan.class);
        TableMeta tMeta = mock(TableMeta.class);
        TableMeta gsiMeta = mock(TableMeta.class);
        ToDrdsRelVisitor visitor = mock(ToDrdsRelVisitor.class);
        PlannerContext plannerContext = mock(PlannerContext.class);
        ExecutionContext executionContext = mock(ExecutionContext.class);
        SchemaManager schemaManager = mock(SchemaManager.class);

        when(scan.getIndexNode()).thenReturn(null);
        when(tMeta.findGlobalSecondaryIndexByName("tablePart")).thenReturn(null);
        when(visitor.getForceIndex(scan)).thenReturn(new SqlIdentifier(Arrays.asList("tablePart", "indexPart"), ZERO));
        when(visitor.buildForceIndexByForceIndex(catalog, scan, schemaName, tMeta, engine)).thenCallRealMethod();
        when(visitor.getPlannerContext()).thenReturn(plannerContext);
        when(plannerContext.getExecutionContext()).thenReturn(executionContext);
        when(executionContext.getSchemaManager(schemaName)).thenReturn(schemaManager);
        when(tMeta.findLocalIndexByName("tablePart")).thenReturn(null);
        when(gsiMeta.findLocalIndexByName("indexPart")).thenReturn(null);

        assertNull(visitor.buildForceIndexByForceIndex(catalog, scan, schemaName, tMeta, engine));
    }

    // gsi local index doesn't exist
    @Test
    public void testBuildForceIndexWithErrorIndexPartNotExist() {
        Engine engine = mock(Engine.class);
        RelOptSchema catalog = mock(RelOptSchema.class);
        LogicalTableScan scan = mock(LogicalTableScan.class);
        TableMeta tMeta = mock(TableMeta.class);
        GsiMetaManager.GsiIndexMetaBean gsi = mock(GsiMetaManager.GsiIndexMetaBean.class);
        TableMeta gsiMeta = mock(TableMeta.class);
        ToDrdsRelVisitor visitor = mock(ToDrdsRelVisitor.class);
        PlannerContext plannerContext = mock(PlannerContext.class);
        ExecutionContext executionContext = mock(ExecutionContext.class);
        SchemaManager schemaManager = mock(SchemaManager.class);

        when(scan.getIndexNode()).thenReturn(null);
        when(tMeta.findGlobalSecondaryIndexByName("tablePart")).thenReturn(gsi);
        when(visitor.getForceIndex(scan)).thenReturn(new SqlIdentifier(Arrays.asList("tablePart", "indexPart"), ZERO));
        when(visitor.buildForceIndexByForceIndex(catalog, scan, schemaName, tMeta, engine)).thenCallRealMethod();
        when(visitor.getPlannerContext()).thenReturn(plannerContext);
        when(plannerContext.getExecutionContext()).thenReturn(executionContext);
        when(executionContext.getSchemaManager(schemaName)).thenReturn(schemaManager);
        when(schemaManager.getTable(gsi.indexTableName)).thenReturn(gsiMeta);
        when(tMeta.findLocalIndexByName("tablePart")).thenReturn(null);
        when(gsiMeta.findLocalIndexByName("indexPart")).thenReturn(null);

        assertNull(visitor.buildForceIndexByForceIndex(catalog, scan, schemaName, tMeta, engine));
    }

    // test force index with invalid argument count
    @Test
    public void testBuildForceIndexWithErrorInvalidArgumentCount() {
        Engine engine = mock(Engine.class);
        RelOptSchema catalog = mock(RelOptSchema.class);
        TableMeta tMeta = mock(TableMeta.class);
        LogicalTableScan scan = mock(LogicalTableScan.class);
        ToDrdsRelVisitor visitor = mock(ToDrdsRelVisitor.class);

        SqlIdentifier invalidIndexId = new SqlIdentifier(Arrays.asList("tablePart", "indexPart", "extraPart"), ZERO);
        doReturn(invalidIndexId).when(visitor).getForceIndex(scan);

        assertNull(visitor.buildForceIndexByForceIndex(catalog, scan, schemaName, tMeta, engine));
    }

    /**
     * 测试没有索引提示时返回null
     */
    @Test
    public void testBuildForceIndexByIndexHintWithoutHint() {
        Engine engine = mock(Engine.class);
        RelOptSchema catalog = mock(RelOptSchema.class);
        TableMeta tMeta = mock(TableMeta.class);
        LogicalTableScan scan = mock(LogicalTableScan.class);

        when(scan.getHints()).thenReturn(null);

        assertNull(new ToDrdsRelVisitor().buildForceIndexByIndexHint(catalog, scan, schemaName, tMeta, engine));
    }

    /**
     * 测试获取到完整索引提示，包括表和索引部分
     */
    @Test
    public void testBuildForceIndexByIndexHintWithFullHintAndIndexPart() {
        Engine engine = mock(Engine.class);
        RelOptSchema catalog = mock(RelOptSchema.class);
        TableMeta tMeta = mock(TableMeta.class);
        LogicalTableScan scan = mock(LogicalTableScan.class);
        SqlCall indexHintMock = mock(SqlCall.class);
        SqlNode tableNode = mock(SqlNode.class);
        SqlNode indexNode = mock(SqlNode.class);

        when(tMeta.getTableName()).thenReturn("tableName_test");
        when(tableNode.toString()).thenReturn("tableName");
        when(indexNode.toString()).thenReturn("indexName");
        when(indexHintMock.getOperandList()).thenReturn(Arrays.asList(mock(SqlNode.class), tableNode, indexNode));
        when(indexHintMock.getOperator()).thenReturn(new SqlSpecialOperator("index", SqlKind.INDEX_OPTION));
        when(scan.getHints()).thenReturn(SqlNodeList.of(indexHintMock));

        assertNull(new ToDrdsRelVisitor().buildForceIndexByIndexHint(catalog, scan, schemaName, tMeta, engine));
    }

    @Test
    /**
     * setIndexNode shouldn't be triggered when there is no index node in scan
     */
    public void testRemoveForceIndexWithoutIndexNode() {
        TableScan tableScan = mock(TableScan.class);
        when(tableScan.getIndexNode()).thenReturn(null);

        ToDrdsRelVisitor visitor = new ToDrdsRelVisitor();
        visitor.removeForceIndex(tableScan);

        verify(tableScan, never()).setIndexNode(any(SqlNodeList.class));
    }

    @Test
    /**
     * setIndexNode shouldn't be triggered when index node in scan is not one SqlNodeList
     */
    public void testRemoveForceIndexWithNonListNode() {
        TableScan tableScan = mock(TableScan.class);
        SqlNode indexNode = mock(SqlNode.class);
        when(tableScan.getIndexNode()).thenReturn(indexNode);

        ToDrdsRelVisitor visitor = new ToDrdsRelVisitor();
        visitor.removeForceIndex(tableScan);

        verify(tableScan, never()).setIndexNode(any(SqlNodeList.class));
    }

    @Test
    /**
     * setIndexNode shouldn't be triggered when index node in scan is an empty SqlNodeList
     */
    public void testRemoveForceIndexWithEmptyListNode() {
        TableScan tableScan = mock(TableScan.class);
        SqlNodeList nodeList = new SqlNodeList(ZERO);
        when(tableScan.getIndexNode()).thenReturn(nodeList);

        ToDrdsRelVisitor visitor = new ToDrdsRelVisitor();
        visitor.removeForceIndex(tableScan);

        verify(tableScan, never()).setIndexNode(any(SqlNodeList.class));
    }

    @Test
    /**
     * setIndexNode shouldn't be triggered when index node in scan had no force index node
     */
    public void testRemoveForceIndexWithOneNonForceNode() {
        TableScan tableScan = mock(TableScan.class);
        SqlNodeList nodeList = new SqlNodeList(ZERO);
        SqlNode nonForceNode = mock(SqlNode.class);
        nodeList.add(nonForceNode);
        when(tableScan.getIndexNode()).thenReturn(nodeList);

        ToDrdsRelVisitor visitor = new ToDrdsRelVisitor();
        visitor.removeForceIndex(tableScan);

        verify(tableScan, never()).setIndexNode(any(SqlNodeList.class));
    }

    @Test
    /**
     * setIndexNode should be triggered when index node in scan had a force index node
     */
    public void testRemoveForceIndexWithOneForceNode() {
        TableScan tableScan = mock(TableScan.class);
        SqlNodeList nodeList = new SqlNodeList(ZERO);
        SqlNode forceNode = buildForceIndexForTest();
        nodeList.add(forceNode);
        when(tableScan.getIndexNode()).thenReturn(nodeList);

        ToDrdsRelVisitor visitor = new ToDrdsRelVisitor();
        visitor.removeForceIndex(tableScan);

        ArgumentCaptor<SqlNode> argumentCaptor = ArgumentCaptor.forClass(SqlNode.class);
        verify(tableScan, times(1)).setIndexNode(argumentCaptor.capture());
        SqlNode indexNode = argumentCaptor.getValue();
        assertTrue(indexNode == null);
    }

    @Test
    /**
     * setIndexNode should be triggered when index node in scan had force/non-force index nodes
     */
    public void testRemoveForceIndexWithMixedNodes() {
        TableScan tableScan = mock(TableScan.class);
        SqlNodeList nodeList = new SqlNodeList(ZERO);
        SqlNode forceNode1 = buildForceIndexForTest();
        SqlNode forceNode2 = buildForceIndexForTest();
        SqlNode nonForceNode1 = mock(SqlNode.class);
        SqlNode nonForceNode2 = mock(SqlNode.class);
        nodeList.add(forceNode1);
        nodeList.add(nonForceNode1);
        nodeList.add(forceNode2);
        nodeList.add(nonForceNode2);
        when(tableScan.getIndexNode()).thenReturn(nodeList);

        ToDrdsRelVisitor visitor = new ToDrdsRelVisitor();
        visitor.removeForceIndex(tableScan);

        ArgumentCaptor<SqlNode> argumentCaptor = ArgumentCaptor.forClass(SqlNode.class);
        verify(tableScan, times(1)).setIndexNode(argumentCaptor.capture());
        SqlNode indexNode = argumentCaptor.getValue();
        assertTrue(indexNode instanceof SqlNodeList &&
            ((SqlNodeList) indexNode).getList().contains(nonForceNode1) &&
            ((SqlNodeList) indexNode).getList().contains(nonForceNode2) &&
            !((SqlNodeList) indexNode).getList().contains(forceNode1) &&
            !((SqlNodeList) indexNode).getList().contains(forceNode2)
        );
    }

    private SqlNode buildForceIndexForTest() {
        return new SqlIndexHint(SqlLiteral.createCharString("FORCE INDEX", ZERO), null, new SqlNodeList(ZERO), ZERO);
    }
}
