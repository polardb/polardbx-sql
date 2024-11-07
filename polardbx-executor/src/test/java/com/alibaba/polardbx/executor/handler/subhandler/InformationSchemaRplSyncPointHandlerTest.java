package com.alibaba.polardbx.executor.handler.subhandler;

import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.view.VirtualView;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

/**
 * @author yudong
 * @since 2024/6/25 16:25
 **/
public class InformationSchemaRplSyncPointHandlerTest {

    @Test
    public void test() throws SQLException {
        List<Object[]> expectedData = Arrays.asList(
            new Object[] {1L, "111", "222", new Timestamp(System.currentTimeMillis())},
            new Object[] {2L, "333", "444", new Timestamp(System.currentTimeMillis())}
        );

        Connection mockMetaConn = mock(Connection.class);
        PreparedStatement mockStmt = mock(PreparedStatement.class);
        ResultSet mockRs = mock(ResultSet.class);
        when(mockMetaConn.prepareStatement(anyString())).thenReturn(mockStmt);
        when(mockStmt.executeQuery()).thenReturn(mockRs);
        when(mockRs.next()).thenReturn(true, true, false);
        when(mockRs.getLong("ID")).thenReturn((Long) expectedData.get(0)[0]).thenReturn((Long) expectedData.get(1)[0]);
        when(mockRs.getString("PRIMARY_TSO")).thenReturn((String) expectedData.get(0)[1])
            .thenReturn((String) expectedData.get(1)[1]);
        when(mockRs.getString("SECONDARY_TSO")).thenReturn((String) expectedData.get(0)[2])
            .thenReturn((String) expectedData.get(1)[2]);
        when(mockRs.getTimestamp("CREATE_TIME")).thenReturn((Timestamp) expectedData.get(0)[3])
            .thenReturn((Timestamp) expectedData.get(1)[3]);

        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(MetaDbUtil::getConnection).thenReturn(mockMetaConn);
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.hasTable(anyString())).thenReturn(true);
            InformationSchemaRplSyncPointHandler handler = new InformationSchemaRplSyncPointHandler(null);
            VirtualView virtualView = mock(VirtualView.class);
            ExecutionContext executionContext = mock(ExecutionContext.class);
            ArrayResultCursor cursor = new ArrayResultCursor("test");
            handler.handle(virtualView, executionContext, cursor);

            for (int i = 0; i < expectedData.size(); i++) {
                Row row = cursor.getRows().get(i);
                assertArrayEquals(expectedData.get(i), row.getValues().toArray());
            }
        }
    }
}
