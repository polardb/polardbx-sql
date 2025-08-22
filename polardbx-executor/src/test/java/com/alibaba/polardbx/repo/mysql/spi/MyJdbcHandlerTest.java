package com.alibaba.polardbx.repo.mysql.spi;

import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.utils.ITransaction;
import org.junit.Test;

import java.sql.SQLException;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MyJdbcHandlerTest {
    @Test
    public void testHandleParamsMap() throws SQLException {
        ExecutionContext ec = new ExecutionContext();
        ITransaction trans = mock(ITransaction.class);
        ec.setTransaction(trans);
        when(trans.getTransactionClass()).thenReturn(ITransactionPolicy.TransactionClass.TSO);

        MyJdbcHandler myJdbcHandler = new MyJdbcHandler(ec, null);
        IConnection c = mock(IConnection.class);
        when(c.isBytesSqlSupported()).thenReturn(false);
        myJdbcHandler.setConnection(c);
        List<ParameterContext> list = myJdbcHandler.handleParamsMap(null, false);
        assert list.size() == 0;
    }
}
