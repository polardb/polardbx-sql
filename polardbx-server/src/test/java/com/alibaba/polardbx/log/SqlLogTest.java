package com.alibaba.polardbx.log;

import com.alibaba.polardbx.common.jdbc.BatchInsertPolicy;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.executor.InsertSplitter;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.matrix.jdbc.TConnection;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.After;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.HashMap;

import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SqlLogTest {

    @Mock
    private ExecutionContext executionContext;

    @Mock
    private TConnection tConnection;

    private AutoCloseable closeable;

    @Before
    public void setUp() {
        closeable = MockitoAnnotations.openMocks(this);
    }

    @After
    public void close() throws Exception {
        closeable.close();
    }

    @Test
    public void testExecuteSplit() {
        ByteString sql;
        InsertSplitter insertSplitter = new InsertSplitter();
        ParamManager paramManager = new ParamManager(new HashMap());

        // Setup default behavior for mocks if necessary
        when(executionContext.getParamManager()).thenReturn(paramManager);
        when(executionContext.getConnection()).thenReturn(tConnection);
        when(tConnection.getLastInsertId()).thenReturn(0L);
        when(tConnection.getReturnedLastInsertId()).thenReturn(0L);

        String insertSql = "insert into t1 values \n (1,2)";
        while (insertSql.length() < executionContext.getParamManager()
            .getLong(ConnectionParams.MAX_BATCH_INSERT_SQL_LENGTH)) {
            insertSql += ", \n (1,2)";
        }
        sql = ByteString.from(insertSql);

        // Act
        ResultCursor resultCursor = insertSplitter.execute(sql, executionContext, BatchInsertPolicy.SPLIT,
            (ByteString s) -> new ResultCursor(new AffectRowCursor(0)),
            (ByteString s) -> new ResultCursor(new AffectRowCursor(0)));

        Assert.assertNotNull(resultCursor);
        Assert.assertTrue(sql.isMultiLine());

        verify(executionContext, atLeastOnce()).getParams();
    }

}
