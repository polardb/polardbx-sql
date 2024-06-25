package com.alibaba.polardbx.parser;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCancelReplicaCheckTableStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLContinueReplicaCheckTableStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLPauseReplicaCheckTableStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLResetReplicaCheckTableStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLShowReplicaCheckDiffStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLShowReplicaCheckProgressStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLStartReplicaCheckTableStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlChangeMasterStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlChangeReplicationFilterStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlResetSlaveStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlShowSlaveStatusStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlStartSlaveStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlStopSlaveStatement;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.parse.privilege.PrivilegeContext;
import com.alibaba.polardbx.optimizer.parse.visitor.ContextParameters;
import com.alibaba.polardbx.optimizer.parse.visitor.FastSqlToCalciteNodeVisitor;
import com.alibaba.polardbx.rpc.CdcRpcClient;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.junit.Test;

import static org.mockito.Mockito.when;

public class ReplicaParserTest {

    private FastSqlToCalciteNodeVisitor visitor;

    @Test
    // 测试不启用CDC时抛出异常
    public void testVisitReplicaStatementWithoutCdc() {
        try (MockedStatic<CdcRpcClient> ob1 = Mockito.mockStatic(CdcRpcClient.class)) {
            ob1.when(CdcRpcClient::useCdc).thenReturn(false);
            ContextParameters context = new ContextParameters(false);
            ExecutionContext ec = new ExecutionContext();
            visitor = new FastSqlToCalciteNodeVisitor(context, ec);

            try {
                visitor.visit(Mockito.mock(MySqlChangeMasterStatement.class));
            } catch (Exception e) {
                Assert.assertTrue(e instanceof TddlRuntimeException);
                Assert.assertTrue(e.getMessage().contains("replica is not support yet"));
            }

            try {
                visitor.visit(Mockito.mock(MySqlChangeReplicationFilterStatement.class));
            } catch (Exception e) {
                Assert.assertTrue(e instanceof TddlRuntimeException);
                Assert.assertTrue(e.getMessage().contains("replica is not support yet"));
            }

            try {
                visitor.visit(Mockito.mock(MySqlStartSlaveStatement.class));
            } catch (Exception e) {
                Assert.assertTrue(e instanceof TddlRuntimeException);
                Assert.assertTrue(e.getMessage().contains("replica is not support yet"));
            }

            try {
                visitor.visit(Mockito.mock(MySqlStopSlaveStatement.class));
            } catch (Exception e) {
                Assert.assertTrue(e instanceof TddlRuntimeException);
                Assert.assertTrue(e.getMessage().contains("replica is not support yet"));
            }

            try {
                visitor.visit(Mockito.mock(MySqlResetSlaveStatement.class));
            } catch (Exception e) {
                Assert.assertTrue(e instanceof TddlRuntimeException);
                Assert.assertTrue(e.getMessage().contains("replica is not support yet"));
            }

            try {
                visitor.visit(Mockito.mock(MySqlShowSlaveStatusStatement.class));
            } catch (Exception e) {
                Assert.assertTrue(e instanceof TddlRuntimeException);
                Assert.assertTrue(e.getMessage().contains("replica is not support yet"));
            }

            try {
                visitor.visit(Mockito.mock(SQLResetReplicaCheckTableStatement.class));
            } catch (Exception e) {
                Assert.assertTrue(e instanceof TddlRuntimeException);
                Assert.assertTrue(e.getMessage().contains("replica is not support yet"));
            }

            try {
                visitor.visit(Mockito.mock(SQLContinueReplicaCheckTableStatement.class));
            } catch (Exception e) {
                Assert.assertTrue(e instanceof TddlRuntimeException);
                Assert.assertTrue(e.getMessage().contains("replica is not support yet"));
            }

            try {
                visitor.visit(Mockito.mock(SQLCancelReplicaCheckTableStatement.class));
            } catch (Exception e) {
                Assert.assertTrue(e instanceof TddlRuntimeException);
                Assert.assertTrue(e.getMessage().contains("replica is not support yet"));
            }

            try {
                visitor.visit(Mockito.mock(SQLPauseReplicaCheckTableStatement.class));
            } catch (Exception e) {
                Assert.assertTrue(e instanceof TddlRuntimeException);
                Assert.assertTrue(e.getMessage().contains("replica is not support yet"));
            }

            try {
                visitor.visit(Mockito.mock(SQLStartReplicaCheckTableStatement.class));
            } catch (Exception e) {
                Assert.assertTrue(e instanceof TddlRuntimeException);
                Assert.assertTrue(e.getMessage().contains("replica is not support yet"));
            }

            try {
                visitor.visit(Mockito.mock(SQLShowReplicaCheckProgressStatement.class));
            } catch (Exception e) {
                Assert.assertTrue(e instanceof TddlRuntimeException);
                Assert.assertTrue(e.getMessage().contains("replica is not support yet"));
            }

            try {
                visitor.visit(Mockito.mock(SQLShowReplicaCheckDiffStatement.class));
            } catch (Exception e) {
                Assert.assertTrue(e instanceof TddlRuntimeException);
                Assert.assertTrue(e.getMessage().contains("replica is not support yet"));
            }
        }
    }

    @Test
    // 测试非上帝用户执行命令时抛出异常
    public void testVisitReplicaStatementWithNonGodUser() {
        try (MockedStatic<CdcRpcClient> ob1 = Mockito.mockStatic(CdcRpcClient.class)) {
            ob1.when(CdcRpcClient::useCdc).thenReturn(true);
            ExecutionContext mockEc = Mockito.mock(ExecutionContext.class);
            when(mockEc.isGod()).thenReturn(false);
            PrivilegeContext mockPc = Mockito.mock(PrivilegeContext.class);
            when(mockPc.getUser()).thenReturn("testUser");
            when(mockPc.getHost()).thenReturn("testHost");
            when(mockEc.getPrivilegeContext()).thenReturn(mockPc);
            ContextParameters context = new ContextParameters(false);
            visitor = new FastSqlToCalciteNodeVisitor(context, mockEc);

            try {
                visitor.visit(Mockito.mock(MySqlChangeMasterStatement.class));
            } catch (Exception e) {
                Assert.assertTrue(e instanceof TddlRuntimeException);
                Assert.assertTrue(e.getMessage().contains("User testUser@'testHost' does not have 'GOD' privilege. "));
            }

            try {
                visitor.visit(Mockito.mock(MySqlChangeReplicationFilterStatement.class));
            } catch (Exception e) {
                Assert.assertTrue(e instanceof TddlRuntimeException);
                Assert.assertTrue(e.getMessage().contains("User testUser@'testHost' does not have 'GOD' privilege. "));
            }

            try {
                visitor.visit(Mockito.mock(MySqlStartSlaveStatement.class));
            } catch (Exception e) {
                Assert.assertTrue(e instanceof TddlRuntimeException);
                Assert.assertTrue(e.getMessage().contains("User testUser@'testHost' does not have 'GOD' privilege. "));
            }

            try {
                visitor.visit(Mockito.mock(MySqlStopSlaveStatement.class));
            } catch (Exception e) {
                Assert.assertTrue(e instanceof TddlRuntimeException);
                Assert.assertTrue(e.getMessage().contains("User testUser@'testHost' does not have 'GOD' privilege. "));
            }

            try {
                visitor.visit(Mockito.mock(MySqlResetSlaveStatement.class));
            } catch (Exception e) {
                Assert.assertTrue(e instanceof TddlRuntimeException);
                Assert.assertTrue(e.getMessage().contains("User testUser@'testHost' does not have 'GOD' privilege. "));
            }

            try {
                visitor.visit(Mockito.mock(SQLResetReplicaCheckTableStatement.class));
            } catch (Exception e) {
                Assert.assertTrue(e instanceof TddlRuntimeException);
                Assert.assertTrue(e.getMessage().contains("User testUser@'testHost' does not have 'GOD' privilege. "));
            }

            try {
                visitor.visit(Mockito.mock(SQLContinueReplicaCheckTableStatement.class));
            } catch (Exception e) {
                Assert.assertTrue(e instanceof TddlRuntimeException);
                Assert.assertTrue(e.getMessage().contains("User testUser@'testHost' does not have 'GOD' privilege. "));
            }

            try {
                visitor.visit(Mockito.mock(SQLCancelReplicaCheckTableStatement.class));
            } catch (Exception e) {
                Assert.assertTrue(e instanceof TddlRuntimeException);
                Assert.assertTrue(e.getMessage().contains("User testUser@'testHost' does not have 'GOD' privilege. "));
            }

            try {
                visitor.visit(Mockito.mock(SQLPauseReplicaCheckTableStatement.class));
            } catch (Exception e) {
                Assert.assertTrue(e instanceof TddlRuntimeException);
                Assert.assertTrue(e.getMessage().contains("User testUser@'testHost' does not have 'GOD' privilege. "));
            }

            try {
                visitor.visit(Mockito.mock(SQLStartReplicaCheckTableStatement.class));
            } catch (Exception e) {
                Assert.assertTrue(e instanceof TddlRuntimeException);
                Assert.assertTrue(e.getMessage().contains("User testUser@'testHost' does not have 'GOD' privilege. "));
            }
        }
    }

    @Test
    public void testVisitShowSlaveStatusStatementWithNonGodUser() {
        try (MockedStatic<CdcRpcClient> ob1 = Mockito.mockStatic(CdcRpcClient.class)) {
            ob1.when(CdcRpcClient::useCdc).thenReturn(true);
            ExecutionContext mockEc = Mockito.mock(ExecutionContext.class);
            when(mockEc.isGod()).thenReturn(false);
            PrivilegeContext mockPc = Mockito.mock(PrivilegeContext.class);
            when(mockPc.getUser()).thenReturn("testUser");
            when(mockPc.getHost()).thenReturn("testHost");
            when(mockEc.getPrivilegeContext()).thenReturn(mockPc);
            ContextParameters context = new ContextParameters(false);
            visitor = new FastSqlToCalciteNodeVisitor(context, mockEc);

            try {
                visitor.visit(Mockito.mock(MySqlShowSlaveStatusStatement.class));
            } catch (Exception e) {
                Assert.assertTrue(!e.getMessage().contains("User testUser@'testHost' does not have 'GOD' privilege. "));
            }
        }
    }

    @Test
    // 测试非上帝用户执行命令时抛出异常
    public void testVisitReplicaStatementWithGodUser() {
        try (MockedStatic<CdcRpcClient> ob1 = Mockito.mockStatic(CdcRpcClient.class)) {
            ob1.when(CdcRpcClient::useCdc).thenReturn(true);
            ExecutionContext mockEc = Mockito.mock(ExecutionContext.class);
            when(mockEc.isGod()).thenReturn(true);
            PrivilegeContext mockPc = Mockito.mock(PrivilegeContext.class);
            when(mockPc.getUser()).thenReturn("testUser");
            when(mockPc.getHost()).thenReturn("testHost");
            when(mockEc.getPrivilegeContext()).thenReturn(mockPc);
            ContextParameters context = new ContextParameters(false);
            visitor = new FastSqlToCalciteNodeVisitor(context, mockEc);

            try {
                visitor.visit(Mockito.mock(MySqlChangeMasterStatement.class));
            } catch (Exception e) {
                Assert.assertTrue(e instanceof TddlRuntimeException);
                Assert.assertTrue(!e.getMessage().contains("User testUser@'testHost' does not have 'GOD' privilege. "));
            }

            try {
                visitor.visit(Mockito.mock(MySqlChangeReplicationFilterStatement.class));
            } catch (Exception e) {
                Assert.assertTrue(e instanceof TddlRuntimeException);
                Assert.assertTrue(!e.getMessage().contains("User testUser@'testHost' does not have 'GOD' privilege. "));
            }

            try {
                visitor.visit(Mockito.mock(MySqlStartSlaveStatement.class));
            } catch (Exception e) {
                Assert.assertTrue(e instanceof TddlRuntimeException);
                Assert.assertTrue(!e.getMessage().contains("User testUser@'testHost' does not have 'GOD' privilege. "));
            }

            try {
                visitor.visit(Mockito.mock(MySqlStopSlaveStatement.class));
            } catch (Exception e) {
                Assert.assertTrue(e instanceof TddlRuntimeException);
                Assert.assertTrue(!e.getMessage().contains("User testUser@'testHost' does not have 'GOD' privilege. "));
            }

            try {
                visitor.visit(Mockito.mock(MySqlResetSlaveStatement.class));
            } catch (Exception e) {
                Assert.assertTrue(e instanceof TddlRuntimeException);
                Assert.assertTrue(!e.getMessage().contains("User testUser@'testHost' does not have 'GOD' privilege. "));
            }

            try {
                visitor.visit(Mockito.mock(SQLResetReplicaCheckTableStatement.class));
            } catch (Exception e) {
                Assert.assertTrue(e instanceof TddlRuntimeException);
                Assert.assertTrue(!e.getMessage().contains("User testUser@'testHost' does not have 'GOD' privilege. "));
            }

            try {
                visitor.visit(Mockito.mock(SQLContinueReplicaCheckTableStatement.class));
            } catch (Exception e) {
                Assert.assertTrue(e instanceof TddlRuntimeException);
                Assert.assertTrue(!e.getMessage().contains("User testUser@'testHost' does not have 'GOD' privilege. "));
            }

            try {
                visitor.visit(Mockito.mock(SQLCancelReplicaCheckTableStatement.class));
            } catch (Exception e) {
                Assert.assertTrue(e instanceof TddlRuntimeException);
                Assert.assertTrue(!e.getMessage().contains("User testUser@'testHost' does not have 'GOD' privilege. "));
            }

            try {
                visitor.visit(Mockito.mock(SQLPauseReplicaCheckTableStatement.class));
            } catch (Exception e) {
                Assert.assertTrue(e instanceof TddlRuntimeException);
                Assert.assertTrue(!e.getMessage().contains("User testUser@'testHost' does not have 'GOD' privilege. "));
            }

            try {
                visitor.visit(Mockito.mock(SQLStartReplicaCheckTableStatement.class));
            } catch (Exception e) {
                Assert.assertTrue(e instanceof TddlRuntimeException);
                Assert.assertTrue(!e.getMessage().contains("User testUser@'testHost' does not have 'GOD' privilege. "));
            }

            try {
                visitor.visit(Mockito.mock(MySqlShowSlaveStatusStatement.class));
            } catch (Exception e) {
                Assert.assertTrue(!e.getMessage().contains("User testUser@'testHost' does not have 'GOD' privilege. "));
            }
        }
    }

}
