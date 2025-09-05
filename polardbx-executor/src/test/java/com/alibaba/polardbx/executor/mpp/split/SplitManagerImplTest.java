package com.alibaba.polardbx.executor.mpp.split;

import com.alibaba.polardbx.common.jdbc.BytesSql;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.executor.gms.ColumnarManager;
import com.alibaba.polardbx.executor.gms.util.ColumnarTransactionUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.LookupSql;
import com.alibaba.polardbx.optimizer.core.rel.OSSTableScan;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableScanBuilder;
import com.alibaba.polardbx.optimizer.utils.IColumnarTransaction;
import com.alibaba.polardbx.optimizer.utils.ITransaction;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import static com.alibaba.polardbx.common.jdbc.ITransactionPolicy.TransactionClass.AUTO_COMMIT;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class SplitManagerImplTest {
    @InjectMocks
    private SplitManagerImpl splitManager;

    @Mock
    private OSSTableScan ossTableScan;

    @Mock
    private ParamManager paramManager;

    @Mock
    private ColumnarManager columnarManager;

    private AutoCloseable autoCloseable;
    private ExecutionContext executionContext;

    @Mock
    private LogicalView logicalView;

    @Mock
    private PhyTableOperation phyTableOperation;

    @Before
    public void setUp() {
        autoCloseable = MockitoAnnotations.openMocks(this);
        executionContext = new ExecutionContext();
        executionContext.setParamManager(paramManager);
        when(columnarManager.latestTso()).thenReturn(233L);
    }

    @After
    public void tearDown() throws Exception {
        if (autoCloseable != null) {
            autoCloseable.close();
        }
    }

    @Test
    public void testFetchTsoAndInitTransaction1() {
        IColumnarTransaction columnarTransaction = mock(IColumnarTransaction.class);
        executionContext.setTransaction(columnarTransaction);
        when(ossTableScan.isColumnarIndex()).thenReturn(true);
        when(ossTableScan.getFlashbackQueryTso(executionContext)).thenReturn(null);
        when(columnarTransaction.snapshotSeqIsEmpty()).thenReturn(false);
        when(columnarTransaction.getSnapshotSeq()).thenReturn(100L);

        Long tso = splitManager.fetchTsoAndInitTransaction(ossTableScan, executionContext);

        assertEquals(Long.valueOf(100L), tso);
    }

    @Test
    public void testFetchTsoAndInitTransaction2() {
        IColumnarTransaction columnarTransaction = mock(IColumnarTransaction.class);
        executionContext.setTransaction(columnarTransaction);
        when(ossTableScan.isColumnarIndex()).thenReturn(true);
        when(ossTableScan.getFlashbackQueryTso(executionContext)).thenReturn(null);
        when(columnarTransaction.snapshotSeqIsEmpty()).thenReturn(true);
        when(paramManager.getBoolean(ConnectionParams.USE_LATEST_COLUMNAR_TSO)).thenReturn(true);

        try (MockedStatic<ColumnarTransactionUtils> mockedStatic = Mockito.mockStatic(ColumnarTransactionUtils.class)) {
            mockedStatic.when(ColumnarTransactionUtils::getLatestTsoFromGms).thenReturn(200L);

            Long tso = splitManager.fetchTsoAndInitTransaction(ossTableScan, executionContext);

            assertEquals(Long.valueOf(200L), tso);
        }
    }

    @Test
    public void testFetchTsoAndInitTransaction3() {
        IColumnarTransaction columnarTransaction = mock(IColumnarTransaction.class);
        executionContext.setTransaction(columnarTransaction);
        when(columnarTransaction.snapshotSeqIsEmpty()).thenReturn(true);
        when(ossTableScan.isColumnarIndex()).thenReturn(true);
        when(ossTableScan.getFlashbackQueryTso(executionContext)).thenReturn(null);
        when(paramManager.getBoolean(ConnectionParams.USE_LATEST_COLUMNAR_TSO)).thenReturn(false);

        try (MockedStatic<ColumnarManager> staticColumnarManager = mockStatic(ColumnarManager.class)) {
            staticColumnarManager.when(ColumnarManager::getInstance).thenReturn(columnarManager);

            Long tso = splitManager.fetchTsoAndInitTransaction(ossTableScan, executionContext);
            assertEquals(Long.valueOf(233L), tso);
        }
    }

    @Test
    public void testFetchTsoAndInitTransaction4() {
        IColumnarTransaction columnarTransaction = mock(IColumnarTransaction.class);
        executionContext.setTransaction(columnarTransaction);
        when(ossTableScan.isColumnarIndex()).thenReturn(true);
        when(ossTableScan.getFlashbackQueryTso(executionContext)).thenReturn(400L);

        Long tso = splitManager.fetchTsoAndInitTransaction(ossTableScan, executionContext);

        assertEquals(Long.valueOf(400L), tso);
    }

    @Test
    public void testFetchTsoAndInitTransaction5() {
        ITransaction nonColumnarTransaction = mock(ITransaction.class);
        when(nonColumnarTransaction.getTransactionClass()).thenReturn(AUTO_COMMIT);
        executionContext.setTransaction(nonColumnarTransaction);
        when(ossTableScan.isColumnarIndex()).thenReturn(true);
        when(ossTableScan.getFlashbackQueryTso(executionContext)).thenReturn(500L);

        Long tso = splitManager.fetchTsoAndInitTransaction(ossTableScan, executionContext);

        assertEquals(Long.valueOf(500L), tso);
    }

    @Test
    public void testFetchTsoAndInitTransaction6() {
        ITransaction nonColumnarTransaction = mock(ITransaction.class);
        when(nonColumnarTransaction.getTransactionClass()).thenReturn(AUTO_COMMIT);
        executionContext.setTransaction(nonColumnarTransaction);
        when(ossTableScan.isColumnarIndex()).thenReturn(true);
        when(ossTableScan.getFlashbackQueryTso(executionContext)).thenReturn(null);
        when(paramManager.getBoolean(ConnectionParams.USE_LATEST_COLUMNAR_TSO)).thenReturn(true);

        try (MockedStatic<ColumnarTransactionUtils> mockedStatic = Mockito.mockStatic(ColumnarTransactionUtils.class)) {
            mockedStatic.when(ColumnarTransactionUtils::getLatestTsoFromGms).thenReturn(600L);

            Long tso = splitManager.fetchTsoAndInitTransaction(ossTableScan, executionContext);

            assertEquals(Long.valueOf(600L), tso);
        }
    }

    @Test
    public void testFetchTsoAndInitTransaction7() {
        ITransaction nonColumnarTransaction = mock(ITransaction.class);
        when(nonColumnarTransaction.getTransactionClass()).thenReturn(AUTO_COMMIT);
        executionContext.setTransaction(nonColumnarTransaction);
        when(ossTableScan.isColumnarIndex()).thenReturn(true);
        when(ossTableScan.getFlashbackQueryTso(executionContext)).thenReturn(null);
        when(paramManager.getBoolean(ConnectionParams.USE_LATEST_COLUMNAR_TSO)).thenReturn(false);

        try (MockedStatic<ColumnarManager> staticColumnarManager = mockStatic(ColumnarManager.class)) {
            staticColumnarManager.when(ColumnarManager::getInstance).thenReturn(columnarManager);

            Long tso = splitManager.fetchTsoAndInitTransaction(ossTableScan, executionContext);
            assertEquals(Long.valueOf(233L), tso);
        }
    }

    @Test
    public void testGenerateLookupSql1() {
        PhyTableScanBuilder phyOperationBuilder = mock(PhyTableScanBuilder.class);
        when(logicalView.isMGetEnabled()).thenReturn(false);
        when(phyTableOperation.getPhyOperationBuilder()).thenReturn(phyOperationBuilder);
        when(phyOperationBuilder.buildPhysicalOrderByClause()).thenReturn("order id");
        when(phyTableOperation.getBytesSql()).thenReturn(BytesSql.getBytesSql("select * from t1"));
        LookupSql sql = splitManager.generateLookupSql(logicalView, phyTableOperation);
        assertEquals(sql.orderBy, "order id");
    }

    @Test
    public void testGenerateLookupSql2() throws SqlParseException {
        String sql = "Select * from t where id = 2";
        SqlNode select = parse(sql);

        PhyTableScanBuilder phyOperationBuilder = mock(PhyTableScanBuilder.class);
        when(logicalView.isMGetEnabled()).thenReturn(true);
        when(logicalView.isInToUnionAll()).thenReturn(false);
        when(phyTableOperation.getPhyOperationBuilder()).thenReturn(phyOperationBuilder);
        when(phyOperationBuilder.buildPhysicalOrderByClause()).thenReturn("order id");
        when(phyTableOperation.getBytesSql()).thenReturn(BytesSql.getBytesSql("select * from t1"));

        when(phyTableOperation.getNativeSqlNode()).thenReturn(select);
        LookupSql lookupSql = splitManager.innerGenerateLookupSql(logicalView, phyTableOperation);
        assertEquals(lookupSql.bytesSql.toString(), "SELECT *\n"
            + "FROM `T`\n"
            + "WHERE ((`ID` = 2) AND ('bka_magic' = 'bka_magic'))");
    }

    @Test
    public void testGenerateLookupSql3() throws SqlParseException {
        String sql = "Select * from t where id = 2";
        SqlNode select = parse(sql);

        PhyTableScanBuilder phyOperationBuilder = mock(PhyTableScanBuilder.class);
        when(logicalView.isMGetEnabled()).thenReturn(true);
        when(logicalView.isInToUnionAll()).thenReturn(true);
        when(phyTableOperation.getPhyOperationBuilder()).thenReturn(phyOperationBuilder);
        when(phyOperationBuilder.buildPhysicalOrderByClause()).thenReturn("order id");
        when(phyTableOperation.getBytesSql()).thenReturn(BytesSql.getBytesSql("select * from t1"));

        when(phyTableOperation.getNativeSqlNode()).thenReturn(select);
        LookupSql lookupSql = splitManager.innerGenerateLookupSql(logicalView, phyTableOperation);
        assertEquals(lookupSql.bytesSql.toString(), "select * from t1");
    }

    private SqlNode parse(String sql) throws SqlParseException {
        SqlParser parser = SqlParser.create(sql);
        return parser.parseQuery();
    }
}