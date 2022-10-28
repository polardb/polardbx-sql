/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.transaction;

import com.alibaba.polardbx.common.constants.IsolationLevel;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.utils.thread.ExecutorUtil;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.group.jdbc.TGroupDirectConnection;
import com.alibaba.polardbx.optimizer.biv.MockConnection;
import com.alibaba.polardbx.optimizer.biv.MockFastDataSource;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.utils.ITransaction;
import com.alibaba.polardbx.transaction.async.AsyncTaskQueue;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.api.support.membermodification.MemberModifier;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.lang.reflect.Field;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;

/**
 * 在非 share read view 的场景下
 * 测试 TransactionConnectionHolder 复用连接的正确性
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({XATransaction.class, TGroupDirectConnection.class})
public class TrxConnHolderTest {

    private static final String schema = "test_schema";
    private static final String[] groups =
        new String[] {"group_000000", "group_000001", "group_000002", "group_000003"};
    private static final Field heldConnParticipatedField;

    static {
        try {
            heldConnParticipatedField = TransactionConnectionHolder.HeldConnection.
                class.getDeclaredField("participated");
            heldConnParticipatedField.setAccessible(true);
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }

    private AbstractTransaction trx;
    private TGroupDataSource tGroupDataSource;
    private boolean shareReadView;
    private long groupParallelism = 1L;
    private Map<String, List<TransactionConnectionHolder.HeldConnection>> groupHeldReadConns;

    //private Map<String, TransactionConnectionHolder.HeldConnection> heldWriteConn;

    private Map<String, TransactionConnectionHolder.WriteHeldConnectionContext> groupWriteHeldConnCtxMap;

    private List<String> errorMsgs;

    public void init(boolean shareReadView, long groupParallelism) throws Exception {
        this.shareReadView = shareReadView;
        this.groupParallelism = groupParallelism;
        this.trx = getMockTrx(shareReadView, groupParallelism);
        this.tGroupDataSource = getMockTGroupDatasource();

//        Field heldWriteConnField = TransactionConnectionHolder.class.getDeclaredField("groupHeldWriteConn");
//        heldWriteConnField.setAccessible(true);

        Field heldWriteConnField = TransactionConnectionHolder.class.getDeclaredField("groupWriteHeldConnCtxMap");
        heldWriteConnField.setAccessible(true);

        Field groupHeldReadConnsField = TransactionConnectionHolder.class.getDeclaredField("groupHeldReadConns");
        Field trxFiled = TransactionConnectionHolder.class.getDeclaredField("trx");
        groupHeldReadConnsField.setAccessible(true);
        trxFiled.setAccessible(true);

        this.groupHeldReadConns = (Map<String, List<TransactionConnectionHolder.HeldConnection>>)
            groupHeldReadConnsField.get(trx.connectionHolder);

//        this.heldWriteConn =
//            (Map<String, TransactionConnectionHolder.HeldConnection>) heldWriteConnField.get(trx.connectionHolder);

        this.groupWriteHeldConnCtxMap =
            (Map<String, TransactionConnectionHolder.WriteHeldConnectionContext>) heldWriteConnField.get(
                trx.connectionHolder);

        this.errorMsgs = Collections.synchronizedList(new ArrayList<>());
    }

    private TGroupDataSource getMockTGroupDatasource() throws Exception {
        TGroupDataSource tGroupDataSource = PowerMockito.mock(TGroupDataSource.class);
        MockFastDataSource mockFastDataSource = new MockFastDataSource();

        PowerMockito.when(tGroupDataSource.getConnection(any())).thenAnswer(
            (Answer<TGroupDirectConnection>) invocation -> {
                TGroupDirectConnection tGroupDirectConnection = PowerMockito.mock(TGroupDirectConnection.class);
                MockConnection connection = new MockConnection(mockFastDataSource);
                PowerMockito.when(tGroupDirectConnection.isClosed()).thenAnswer(new Answer<Boolean>() {
                    @Override
                    public Boolean answer(InvocationOnMock invocation) throws Throwable {
                        return connection.isClosed();
                    }
                });
                PowerMockito.doReturn(connection).when(tGroupDirectConnection).getConn();
                PowerMockito.doAnswer(invocation1 -> {
                    connection.close();
                    return null;
                }).when(tGroupDirectConnection).close();
                return tGroupDirectConnection;
            });
        return tGroupDataSource;
    }

    private AbstractTransaction getMockTrx(boolean shareReadView, long groupParallelism) throws Exception {
        ExecutionContext executionContext = new ExecutionContext();
        executionContext.setTxIsolation(IsolationLevel.REPEATABLE_READ.getCode());
        executionContext.setGroupParallelism(groupParallelism);
        executionContext.setShareReadView(shareReadView);
        // 注入TransactionManager
        TransactionManager trxManager = PowerMockito.mock(TransactionManager.class);
        PowerMockito.doCallRealMethod().when(trxManager).getTransactionExecutor();
        TransactionExecutor transactionExecutor = PowerMockito.mock(TransactionExecutor.class);
        PowerMockito.doCallRealMethod().when(transactionExecutor).getAsyncQueue();

        MemberModifier.field(TransactionExecutor.class, "asyncQueue").set(transactionExecutor,
            PowerMockito.spy(new AsyncTaskQueue(schema, ExecutorUtil.create("ServerExecutor", 10))));
        MemberModifier.field(TransactionManager.class, "executor").set(trxManager, transactionExecutor);

        XATransaction trx = PowerMockito.spy(new XATransaction(executionContext, trxManager));
        PowerMockito.doNothing().when(trx).begin(anyString(), anyString(), any());
        PowerMockito.doNothing().when(trx, "beginRwTransaction", Mockito.any(String.class));
        PowerMockito.doNothing().when(trx).cleanupAllConnections();

        // TransactionConnectionHolder 初始化引用了 this
        // 需要 hook 成 Mock的trx对象
        Whitebox.setInternalState(trx.connectionHolder, "trx", trx);
        return trx;
    }

    @Test
    public void testWriteConnReuseSerially() throws Exception {
        init(false, 1L);
        doTestSerially();
    }

    @Test
    public void testWriteConnReuseSeriallyWithReadView() throws Exception {
        init(true, 1L);
        doTestSerially();
    }

    @Test
    public void testWriteConnReuseConcurrently() throws Exception {
        init(false, 1L);
        doTestConcurrently();
    }

    @Test
    public void testWriteConnReuseConcurrentlyWithReadView() throws Exception {
        init(true, 1L);
        doTestConcurrently();
    }

    @Test
    public void testWriteConnReuseSerially2() throws Exception {
        init(false, 8L);
        doTestSerially();
    }

    @Test
    public void testWriteConnReuseSeriallyWithReadView2() throws Exception {
        init(true, 8L);
        doTestSerially();
    }

    @Test
    public void testWriteConnReuseConcurrently2() throws Exception {
        init(false, 8L);
        doTestConcurrently();
    }

    @Test
    public void testWriteConnReuseConcurrentlyWithReadView2() throws Exception {
        init(true, 8L);
        doTestConcurrently();
    }

    private boolean enableGroupParallelism() {
        return groupParallelism > 1 && shareReadView;
    }

    private void doTestSerially() throws Exception {
        IConnection readConn0 = getReadConn(groups[0]);
        IConnection readConn1 = getReadConn(groups[1]);

        //Assert.assertTrue(heldWriteConn.isEmpty());
        Assert.assertTrue(groupWriteHeldConnCtxMap.isEmpty());

        verifyGroupHeldReadConnsNum(groups[0], 1);
        verifyGroupHeldReadConnsNum(groups[1], 1);
        verifyGroupHeldReadConnsNum(groups[2], 0);
        verifyGroupHeldReadConnsNum(groups[3], 0);

        IConnection tmpWriteConn0 = null;
        try {
            // 之前的读连接还未结束
            // 获取写连接预期报错
            tmpWriteConn0 = getWriteConn(groups[0]);
            if (!enableGroupParallelism()) {
                Assert.fail("Expect get write connection failed when read conn is in use!");
            } else {
                /**
                 * come here means getWriteConn of group[0] is successful
                 * because trx will create another new share-read-view-conn for getting write conn
                 */
            }
        } catch (TddlRuntimeException e) {
            Assert.assertTrue(e.getMessage().contains("read connection is in use before write"));
        }

        if (enableGroupParallelism()) {
            verifyGroupHeldWriteConnNum(groups[0], 1);
            if (tmpWriteConn0 != null) {
                // 关闭获取的写连接
                trx.tryClose(tmpWriteConn0, groups[0]);
            }
        }

        // 关闭读连接后再获取写连接
        trx.tryClose(readConn0, groups[0]);
        trx.tryClose(readConn1, groups[1]);

        IConnection writeConn0 = getWriteConn(groups[0]);
        //verifyHeldWriteConnNum(1);

        verifyGroupHeldWriteConnNum(groups[0], 1);
        checkHoldingWriteConn(groups[0]);

        if (enableGroupParallelism()) {
            /**
             * Because curr write conn of group[0] is got by creating another new physical conn,
             * instead of converting from a read conn to write conn,
             * so the read conn of group[0] is 1
             */
            verifyGroupHeldReadConnsNum(groups[0], 1);
        } else {
            /**
             * Because curr write conn of group[0] is got by converting a read conn to write conn,
             * so the read conn of group[0] should be 0
             */
            verifyGroupHeldReadConnsNum(groups[0], 0);
        }

        IConnection writeConn1 = getWriteConn(groups[1]);
        verifyHeldWriteConnNum(2);
        checkHoldingWriteConn(groups[1]);
        /**
         * the write conn is got by converting from a read conn to write conn ,so the num of read conn is 0
         */
        verifyGroupHeldReadConnsNum(groups[1], 0);
        verifyHeldWriteConnParticipated(groups[0]);
        verifyHeldWriteConnParticipated(groups[1]);

        // 写连接未释放的group不能再获取读/写连接
        try {
            getWriteConn(groups[0]);
            Assert.fail("Expect get write connection failed when write conn is in use!");
        } catch (TddlRuntimeException e) {
            assertWriteConnInUseFailure(e);
        }
        try {
            getReadConn(groups[0]);
            Assert.fail("Expect get read connection failed when write conn is in use!");
        } catch (TddlRuntimeException e) {
            assertWriteConnInUseFailure(e);
        }

        // 关闭写连接后再获取读连接
        trx.tryClose(writeConn0, groups[0]);
        readConn0 = getReadConn(groups[0]);
        // 预期复用原写连接
        Assert.assertSame(readConn0, writeConn0);
        verifyHeldWriteConnParticipated(groups[0]);

        // 关闭事务后 持有链接应当被释放
        trx.close();
        checkHoldConnsCleared();
        Assert.assertTrue(writeConn0.isClosed());
        Assert.assertTrue(writeConn1.isClosed());
    }

    private void doTestConcurrently() {
        final int parallelism = 100;
        final int waitTimeoutSeconds = 100;
        final AtomicBoolean[] acquiredWriteConnFlags = new AtomicBoolean[groups.length];
        final AtomicInteger[] groupReadConnCounts = new AtomicInteger[groups.length];
        for (int i = 0; i < groups.length; i++) {
            acquiredWriteConnFlags[i] = new AtomicBoolean(false);
            groupReadConnCounts[i] = new AtomicInteger(0);
        }
        final CountDownLatch countDownLatch = new CountDownLatch(parallelism);
        final CyclicBarrier cyclicBarrier = new CyclicBarrier(parallelism);

        for (int i = 0; i < parallelism; i++) {
            ReadWriteConnTester tester = new ReadWriteConnTester(i % groups.length, acquiredWriteConnFlags,
                groupReadConnCounts, countDownLatch, cyclicBarrier);
            new Thread(tester).start();
        }
        try {
            if (!countDownLatch.await(waitTimeoutSeconds, TimeUnit.SECONDS)) {
                Assert.fail(String.format("Timeout while testing concurrently after %ds", waitTimeoutSeconds));
            }
        } catch (InterruptedException e) {
            // ignore
        }
        Assert.assertTrue(StringUtils.join(errorMsgs, "\n"), errorMsgs.isEmpty());
        // 关闭事务后 持有链接应当被释放
        trx.close();
        checkHoldConnsCleared();
    }

    private void verifyHeldWriteConnParticipated(String group) throws IllegalAccessException {
        TransactionConnectionHolder.ParticipatedState participated = (TransactionConnectionHolder.ParticipatedState)
            heldConnParticipatedField.get(groupWriteHeldConnCtxMap.get(group).getDefaultWriteConn());

        Assert.assertSame("Write connection should be participated", participated,
            TransactionConnectionHolder.ParticipatedState.WRITTEN);
    }

    private void checkHoldConnsCleared() {
        for (String group : groups) {
            verifyGroupHeldReadConnsNum(group, 0);
        }
        verifyHeldWriteConnNum(0);
    }

    private IConnection getReadConn(String group) throws SQLException {
        return trx.getConnection(schema, group, tGroupDataSource, ITransaction.RW.READ, new ExecutionContext());
    }

    private IConnection getReadConn(String group, Long connId) throws SQLException {
        return trx.getConnection(schema, group, tGroupDataSource, ITransaction.RW.READ);
    }

    private IConnection getWriteConn(String group) throws SQLException {
        return trx.getConnection(schema, group, tGroupDataSource, ITransaction.RW.WRITE, new ExecutionContext());
    }

    private void verifyGroupHeldReadConnsNum(String group, int expectNum) {
        Assert.assertEquals(String.format("Expect %d read connection in %s", expectNum, group),
            expectNum, groupHeldReadConns.getOrDefault(group, new ArrayList<>()).size());
    }

    private void verifyGroupHeldWriteConnNum(String group, int expectNum) {
//        Assert.assertEquals(String.format("Expected %d held write conn", expectNum), expectNum, heldWriteConn.size());
        Assert.assertEquals(String.format("Expected %d held write conn", expectNum), expectNum,
            groupWriteHeldConnCtxMap.get(group).allWriteConns().size());
    }

    private void verifyHeldWriteConnNum(int expectNum) {
//        Assert.assertEquals(String.format("Expected %d held write conn", expectNum), expectNum, heldWriteConn.size());
        Assert.assertEquals(String.format("Expected %d held write conn", expectNum), expectNum, allWriteConns());
    }

    private long allWriteConns() {
        AtomicLong num = new AtomicLong(0);
        groupWriteHeldConnCtxMap.entrySet().stream().forEach(e -> num.addAndGet(e.getValue().allWriteConns().size()));
        return num.get();
    }

    private void checkHoldingWriteConn(String group) {
//        Assert.assertNotNull("Expected holding write conn on "
//            + group, heldWriteConn.get(group));
        Assert.assertNotNull("Expected holding write conn on "
            + group, !groupWriteHeldConnCtxMap.get(group).allWriteConns().isEmpty());
    }

    private void assertWriteConnInUseFailure(Exception e) {
        if (!shareReadView) {
            Assert.assertTrue(e.getMessage(), e.getMessage().contains("write connection is in use"));
        } else {
            Assert.assertTrue(e.getMessage(), e.getMessage()
                .contains("write connection is in write state while sharing read view"));
        }
    }

    private class ReadWriteConnTester implements Runnable {
        private final int groupIdx;
        private final AtomicBoolean[] acquiredWriteConnFlags;
        private final AtomicInteger[] groupReadConnCounts;
        private final CountDownLatch countDownLatch;
        private final CyclicBarrier cyclicBarrier;

        public ReadWriteConnTester(int groupIdx,
                                   AtomicBoolean[] acquiredWriteConnFlags,
                                   AtomicInteger[] groupReadConnCounts,
                                   CountDownLatch countDownLatch,
                                   CyclicBarrier cyclicBarrier) {
            this.groupIdx = groupIdx;
            this.acquiredWriteConnFlags = acquiredWriteConnFlags;
            this.groupReadConnCounts = groupReadConnCounts;
            this.countDownLatch = countDownLatch;
            this.cyclicBarrier = cyclicBarrier;
        }

        @Override
        public void run() {
            try {
                String group = groups[groupIdx];
                IConnection readConn0 = null;
                try {
                    readConn0 = testGetReadConn(groupIdx);
                } finally {
                    // 等待读连接拿取过程结束
                    cyclicBarrier.await();
                }

                try {
                    getWriteConnWhileReadConnInUse(groupIdx);
                } finally {
                    // 等待写连接拿取报错结束
                    cyclicBarrier.await();
                }

                if (readConn0 != null) {
                    trx.tryClose(readConn0, group);
                    groupReadConnCounts[groupIdx].decrementAndGet();
                }
                while (groupReadConnCounts[groupIdx].get() != 0) {
                    // 等待读连接结束
                    Thread.sleep(100);
                }

                IConnection writeConn = null;
                try {
                    writeConn = testGetWriteConn(groupIdx);
                } finally {
                    // 等待读写连接混合拿取过程结束
                    cyclicBarrier.await();
                }
                verifyHeldWriteConnParticipated(group);

                if (writeConn != null) {
                    trx.tryClose(writeConn, group);
                }
            } catch (SQLException | InterruptedException | BrokenBarrierException e) {
                Assert.fail(e.getMessage());
            } catch (Throwable e) {
                errorMsgs.add(e.getMessage());
            } finally {
                countDownLatch.countDown();
            }
        }

        /**
         * 混合拿取读写连接
         *
         * @return 该GROUP拿取成功的写连接
         */
        private IConnection testGetWriteConn(int groupIdx) throws SQLException {
            String group = groups[groupIdx];
            synchronized (acquiredWriteConnFlags[groupIdx]) {
                if (acquiredWriteConnFlags[groupIdx].compareAndSet(false, true)) {
                    return getWriteConn(group);
                } else {
                    try {
                        if (groupIdx % 2 == 0) {
                            getReadConn(group);
                            if (!shareReadView) {
                                Assert.fail("Expect get read connection "
                                    + "failed while write conn is in use on " + group);
                            }
                        } else {
                            getWriteConn(group);
                            Assert.fail("Expect get write connection "
                                + "failed while write conn is in use on " + group);
                        }
                    } catch (TddlRuntimeException e) {
                        assertWriteConnInUseFailure(e);
                    }
                }
            }
            return null;
        }

        private IConnection testGetReadConn(int groupIdx) throws SQLException {
            try {
                String group = groups[groupIdx];
                IConnection readConn = getReadConn(group);
                groupReadConnCounts[groupIdx].incrementAndGet();
                return readConn;
            } catch (TddlRuntimeException e) {
                Assert.assertTrue(e.getMessage().contains("multiple read connections on one group is not allowed"));
            }
            return null;
        }

        private void getWriteConnWhileReadConnInUse(int groupIdx) throws SQLException {

            try {
                synchronized (acquiredWriteConnFlags[groupIdx]) {
                    String group = groups[groupIdx];
                    IConnection writeConn = getWriteConn(group);
                    if (!enableGroupParallelism()) {
                        Assert.fail("Expect get write connection failed while read conn is in use!");
                    } else {
                        /**
                         * Come here getting write connection is successful
                         */
                        verifyGroupHeldWriteConnNum(group, 1);
                        if (writeConn != null) {
                            // 关闭获取的写连接
                            trx.tryClose(writeConn, group);
                        }
                    }
                }
            } catch (TddlRuntimeException e) {
                Assert.assertTrue(e.getMessage().contains("read connection is in use before write"));
            }
        }
    }
}
