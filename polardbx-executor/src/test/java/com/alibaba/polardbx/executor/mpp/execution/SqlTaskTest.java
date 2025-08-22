package com.alibaba.polardbx.executor.mpp.execution;

import com.alibaba.polardbx.common.mock.MockUtils;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.mpp.OutputBuffers;
import com.alibaba.polardbx.executor.mpp.Session;
import com.alibaba.polardbx.executor.mpp.execution.buffer.BufferState;
import com.alibaba.polardbx.executor.mpp.metadata.TaskLocation;
import com.alibaba.polardbx.executor.spi.ITransactionManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.memory.MemoryType;
import com.alibaba.polardbx.optimizer.statis.ColumnarTracer;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SqlTaskTest {

    @Test
    public void testTrxIdClean() {
        final String schema = "db1";
        ExecutorService testExecutor = Executors.newSingleThreadExecutor();

        try {

            ExecutorContext context = Mockito.mock(ExecutorContext.class);
            ITransactionManager trxManager = Mockito.mock(ITransactionManager.class);
            Mockito.when(context.getTransactionManager()).thenReturn(trxManager);

            ExecutorContext.setContext(schema, context);

            TaskLocation location = Mockito.mock(TaskLocation.class);
            TestSqlTask sqlTask = new TestSqlTask("1", TaskId.EMPTY_TASKID, location,
                new QueryContext(null, "test"),
                new SqlTaskExecutionFactory(testExecutor, new TaskExecutor(), null, null, null),
                testExecutor);

            // do nothing if trx id is -1
            sqlTask.clean();
            long trxId = sqlTask.getTrxId();
            Assert.assertEquals(-1, trxId);

            SessionRepresentation sessionRepresentation = Mockito.mock(SessionRepresentation.class);
            Mockito.when(sessionRepresentation.getSchema()).thenReturn(schema);
            ExecutionContext executionContext = new ExecutionContext();
            executionContext.setMemoryPool(new MemoryPool("test", 1024, MemoryType.OTHER));
            Session session = new Session("session", executionContext);

            Mockito.when(sessionRepresentation.toSession(Mockito.any(TaskId.class), Mockito.any(QueryContext.class),
                    Mockito.anyLong(), Mockito.nullable(ColumnarTracer.class)))
                .thenReturn(session);
            SqlTask.TaskHolder taskHolder = Mockito.mock(SqlTask.TaskHolder.class);
            Mockito.when(taskHolder.getTaskExecution()).thenReturn(Mockito.mock(SqlTaskExecution.class));
            Mockito.when(taskHolder.getFinalTaskInfo()).thenReturn(Mockito.mock(TaskInfo.class));
            Mockito.when(taskHolder.getIoStats()).thenReturn(Mockito.mock(SqlTaskIoStats.class));
            sqlTask.getTaskHolderReference().set(taskHolder);

            OutputBuffers outputBuffers =
                OutputBuffers.createInitialEmptyOutputBuffers(OutputBuffers.BufferType.BROADCAST);
            sqlTask.updateTask(sessionRepresentation, Optional.empty(), new ArrayList<>(), outputBuffers
                , Optional.empty(), null);
            trxId = sqlTask.getTrxId();
            Assert.assertTrue(trxId > 0);

            sqlTask.failed(new RuntimeException());
            // cannot do update task and the trxId should be cleared
            sqlTask.updateTask(sessionRepresentation, Optional.empty(), new ArrayList<>(),
                outputBuffers, Optional.empty(),
                null);

            waitSqlTaskBufferEnd(sqlTask);
            trxId = sqlTask.getTrxId();
            Assert.assertEquals(-1, trxId);
        } finally {
            ExecutorContext.clearContext(schema);
            testExecutor.shutdownNow();
        }
    }

    @Test
    public void testFailedBeforeUpdate() {
        final String schema = "db1";
        ExecutorService testExecutor = Executors.newSingleThreadExecutor();

        try {
            ExecutorContext context = Mockito.mock(ExecutorContext.class);
            ITransactionManager trxManager = Mockito.mock(ITransactionManager.class);
            Mockito.when(context.getTransactionManager()).thenReturn(trxManager);

            ExecutorContext.setContext(schema, context);

            TaskLocation location = Mockito.mock(TaskLocation.class);
            TestSqlTask sqlTask = new TestSqlTask("1", TaskId.EMPTY_TASKID, location,
                new QueryContext(null, "test"),
                new SqlTaskExecutionFactory(testExecutor, new TaskExecutor(), null, null, null),
                testExecutor);

            SessionRepresentation sessionRepresentation = Mockito.mock(SessionRepresentation.class);
            Mockito.when(sessionRepresentation.getSchema()).thenReturn(schema);
            ExecutionContext executionContext = new ExecutionContext();
            executionContext.setMemoryPool(new MemoryPool("test", 1024, MemoryType.OTHER));
            Session session = new Session("session", executionContext);

            Mockito.when(sessionRepresentation.toSession(Mockito.any(TaskId.class), Mockito.any(QueryContext.class),
                    Mockito.anyLong(), Mockito.nullable(ColumnarTracer.class)))
                .thenReturn(session);
            SqlTask.TaskHolder taskHolder = Mockito.mock(SqlTask.TaskHolder.class);
            Mockito.when(taskHolder.getTaskExecution()).thenReturn(Mockito.mock(SqlTaskExecution.class));
            Mockito.when(taskHolder.getFinalTaskInfo()).thenReturn(Mockito.mock(TaskInfo.class));
            Mockito.when(taskHolder.getIoStats()).thenReturn(Mockito.mock(SqlTaskIoStats.class));
            sqlTask.getTaskHolderReference().set(taskHolder);

            OutputBuffers outputBuffers =
                OutputBuffers.createInitialEmptyOutputBuffers(OutputBuffers.BufferType.BROADCAST);

            sqlTask.failed(new RuntimeException());
            sqlTask.updateTask(sessionRepresentation, Optional.empty(), new ArrayList<>(),
                outputBuffers, Optional.empty(),
                null);

            waitSqlTaskBufferEnd(sqlTask);
            long trxId = sqlTask.getTrxId();
            Assert.assertEquals(-1, trxId);

        } finally {
            ExecutorContext.clearContext(schema);
            testExecutor.shutdownNow();
        }
    }

    private void waitSqlTaskBufferEnd(SqlTask sqlTask) {
        StateMachine<BufferState> state =
            (StateMachine<BufferState>) MockUtils.getInternalState(sqlTask.getOutputBuffer(), "state");
        for (int i = 0; i < 10; i++) {
            if (state.get().isTerminal()) {
                break;
            } else {
                if (i == 9) {
                    throw new RuntimeException("wait for sql task done timeout");
                }
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    static class TestSqlTask extends SqlTask {

        public TestSqlTask(String nodeId, TaskId taskId, TaskLocation location, QueryContext queryContext,
                           SqlTaskExecutionFactory sqlTaskExecutionFactory, ExecutorService taskNotificationExecutor) {
            super(nodeId, taskId, location, queryContext, sqlTaskExecutionFactory, taskNotificationExecutor);
        }

        @Override
        public TaskInfo getInitialTaskInfo() {
            return Mockito.mock(TaskInfo.class);
        }

        @Override
        protected TaskInfo createTaskInfoWithTaskStats(TaskHolder taskHolder) {
            return Mockito.mock(TaskInfo.class);
        }
    }

}
