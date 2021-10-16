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

package com.alibaba.polardbx.executor.mdl;

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.executor.mdl.manager.MdlManagerStamped;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

/**
 * @author chenmo.cm
 */
public class MdlManagerStampedTest {

    private final String schema = "MDL_TEST_APP";
    private final String connId = "mdl_conn_id";
    private final String trxIdPrefix = "mdl_trx_id";
    private final String tableName = "mdl_table_name";
    private final AcceptIdGenerator g = new AcceptIdGenerator();
    private final AtomicInteger trxIdGenerator = new AtomicInteger();
    private final Map<String, Long> tableSchemaMap = new HashMap<>();
    private final int CONN_COUNT = 100;
    private final int TRX_COUNT = 10;
    private MdlManager mdlManager;

    @Before
    public void setUp() throws Exception {
        mdlManager = MdlManagerStamped.getInstance(schema);
        tableSchemaMap.put(tableName, 0L);
    }

    @After
    public void tearDown() throws Exception {
        MdlManagerStamped.removeInstance(schema);
    }

    @Test
    public void testGetMdl() {
        /**
         * ddl
         */
        final ExecutorService ddlPool = Executors.newFixedThreadPool(1);
        ddlPool.submit(() -> {
            while (true) {
                // connect to DRDS
                final FrontConnection frontConn = new FrontConnection(g.nextId());

                final MdlContext context = mdlManager.addContext(frontConn.getId());
                frontConn.setMdlContext(context);

                final Long trxId = 9527L;
                // get lock
                final MdlTicket ticket = context.acquireLock(writeRequest(trxId, tableName));

                // update schema version
                tableSchemaMap.put(tableName, tableSchemaMap.get(tableName) + 1);

                try {
                    TimeUnit.MILLISECONDS.sleep(2);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                // release lock
                context.releaseLock(trxId, ticket);

                // close connection
                mdlManager.removeContext(context);

                if (Thread.interrupted()) {
                    break;
                }
            }
        });

        /**
         * dml
         */
        final ExecutorService dmlPool = Executors.newFixedThreadPool(10);

        final List<Future> dmlFutures = new ArrayList<>();
        IntStream.range(0, 10).forEach(index -> {

            final Future<?> future = dmlPool.submit(() -> {
                final Random r = new Random(LocalDateTime.now().getNano());

                int connCount = 0;
                while (connCount++ < CONN_COUNT) {
                    // connect to DRDS
                    final FrontConnection frontConn = new FrontConnection(g.nextId());

                    final MdlContext context = mdlManager.addContext(frontConn.getId());
                    frontConn.setMdlContext(context);

                    int trxCount = 0;
                    while (trxCount++ < TRX_COUNT) {
                        final int stmtCountInTrx = Math.abs(r.nextInt()) % 10 + 1;

                        final Long trxId = 9527L + trxIdGenerator.getAndIncrement();

                        // begin transaction
                        MdlTicket ticket = null;
                        for (int i = 0; i < stmtCountInTrx; i++) {
                            final MdlTicket newTicket = context.acquireLock(readRequest(trxId, tableName));

                            // grant ticket once for a table in a transaction
                            if (null != ticket) {
                                Assert.assertEquals(ticket, newTicket);
                            }

                            ticket = newTicket;

                            // get schema version
                            final Long schemaVersion1 = tableSchemaMap.get(tableName);

                            // execute query
                            try {
                                TimeUnit.MILLISECONDS.sleep(1);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }

                            // check schema version
                            final Long schemaVersion2 = tableSchemaMap.get(tableName);

                            final long versionChange = schemaVersion2 - schemaVersion1;
                            Assert.assertTrue(versionChange >= 0 && versionChange <= 1);
                        }

                        // commit
                        context.releaseLock(trxId, ticket);

                        Assert.assertFalse(ticket.isValidate());
                    }

                    // close connection
                    final MdlContext mdlContext = mdlManager.removeContext(context);
                }
            });

            dmlFutures.add(future);
        });

        dmlFutures.forEach(future -> {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        });

        ddlPool.shutdown();

        System.out.println(JSON.toJSONString(tableSchemaMap));
    }

    private MdlContext createConnection(String connId) {
        return null;
    }

    private void closeConnection(String connId) {

    }

    private MdlRequest readRequest(Long trxId, String tableName) {

        return new MdlRequest(trxId,
            MdlKey.getTableKeyWithLowerTableName(schema, tableName),
            MdlType.MDL_SHARED_WRITE,
            MdlDuration.MDL_TRANSACTION);
    }

    private MdlRequest writeRequest(Long trxId, String tableName) {

        return new MdlRequest(trxId,
            MdlKey.getTableKeyWithLowerTableName(schema, tableName),
            MdlType.MDL_EXCLUSIVE,
            MdlDuration.MDL_TRANSACTION);
    }

    private static class FrontConnection {

        private final long id;
        private MdlContext mdlContext;

        public FrontConnection(long id) {
            this.id = id;
        }

        public long getId() {
            return id;
        }

        public MdlContext getMdlContext() {
            return mdlContext;
        }

        public void setMdlContext(MdlContext mdlContext) {
            this.mdlContext = mdlContext;
        }
    }

    /**
     * 前端连接ID生成器
     *
     * @author xianmao.hexm
     */
    private static class AcceptIdGenerator {

        private static final long MAX_VALUE = 0xffffffffL;
        private final Object lock = new Object();
        // ManagerConnection IDs are generated by AcceptIdGenerator,
        // ServerConnection IDs are generated by ClusterAcceptIdGenerator,
        // Both Manager Connections and Server Connections are put into
        // frontends(a map whose key is connection ID) of NIOProcessor.java.
        // So ManagerConnection ID should start from an offset to make sure no
        // Manager Connection ID would be same as ServerConnection ID
        private long acceptId = 0xefffffffL;

        private long nextId() {
            synchronized (lock) {
                if (acceptId >= MAX_VALUE) {
                    acceptId = 0L;
                }
                return ++acceptId;
            }
        }
    }
}
