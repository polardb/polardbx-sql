package com.alibaba.polardbx.qatest.ddl.online.mdl;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.qatest.twoPhaseDdl.TwoPhaseDdlTestUtils.DdlStateCheckUtil;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.Lists;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PreemptiveTimeTestBase {
    public static void executeConcurrentDml(Logger logger, Connection connection, String schemaName, String dml,
                                            long timeDelayInMs, Boolean expectedDmlSuccess, String errMsg)
        throws InterruptedException {
        String useDbSql = "use " + schemaName;

        JdbcUtil.executeUpdateSuccess(connection, useDbSql);
        logger.info(" dml thread: begin ");
        JdbcUtil.executeUpdateSuccess(connection, "begin");
        logger.info(" dml thread: start dml " + dml);
        JdbcUtil.executeUpdateSuccess(connection, dml);
        logger.info(" dml thread: sleep for " + timeDelayInMs + " ms");
        Thread.sleep(timeDelayInMs);
        logger.info(" dml thread: commit");
        if (expectedDmlSuccess) {
            JdbcUtil.executeUpdateSuccess(connection, "commit");
            logger.info(" dml thread: commit success, EXPECTED");
        } else {
            JdbcUtil.executeUpdateFailed(connection, "commit", errMsg);
            logger.info(" dml thread: commit failed for " + errMsg + ", EXPECTED");
        }
    }

    public static void emitDmlConcurrent(Logger logger, Lock lock, Connection connection, int i,
                                         AtomicInteger totalCount, AtomicBoolean killed, int maxCount, String dml,
                                         String ddl,
                                         AtomicLong jobId,
                                         long timeDelayInMs, Boolean expectedDmlSuccess, String errMsg)
        throws InterruptedException {
        Boolean connectionKilled = false;
        do {
            try {
                lock.lock();
                if (connectionKilled || (jobId.get() != -1 && DdlStateCheckUtil.checkIfTerminate(connection, jobId.get()))
                    || totalCount.get() >= maxCount) {
                    lock.unlock();
                    break;
                }
                String threadString = "dml thread " + i;
                logger.info(threadString + "  begin ");
                JdbcUtil.executeUpdateSuccess(connection, "begin");
                logger.info(threadString + " start dml " + dml);
                JdbcUtil.executeUpdateSuccess(connection, dml);
                if (jobId.get() == -1L) {
                    try {
                        jobId.set(DdlStateCheckUtil.getDdlJobIdFromPattern(connection, ddl));
                    }catch (Exception | AssertionError e) {
                        if(e.getMessage().contains("Communications link failure")){
                            connectionKilled = true;
                            killed.set(true);
                        }
                    }
                }
                long firstStop = Math.min(timeDelayInMs - 2000, 14000);
                logger.info(threadString + " sleep for " + firstStop + " ms");
                Thread.sleep(firstStop);
                totalCount.incrementAndGet();
                lock.unlock();
                logger.info(threadString + " sleep for " + (timeDelayInMs - firstStop) + " ms");
                Thread.sleep(timeDelayInMs - firstStop);

                logger.info(threadString + " commit");
                if (expectedDmlSuccess) {
                    if(connectionKilled){
                        throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, " expected success, but error");
                    }
                    JdbcUtil.executeUpdateSuccess(connection, "commit");
                    logger.info(threadString + " commit success, EXPECTED");
                } else {
                    if(!connectionKilled) {

                        try {
                            Statement stmt = connection.createStatement();
                            stmt.execute("commit");
                        } catch (Exception e) {
                            if (e.getMessage().contains(errMsg) || e.getMessage()
                                .contains("Could not retrieve transation read-only status server")) {
                                logger.info(threadString + " commit failed for " + errMsg + ", EXPECTED");
                                connectionKilled = true;
                                killed.set(true);
                            }
                        }
                    }
                    if (!connectionKilled && !killed.get()) {
                        throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, " expected error, but success");
                    }
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } finally {
//                lock.unlock();
            }

            if (connectionKilled || DdlStateCheckUtil.checkIfTerminate(connection, jobId.get())
                || totalCount.get() >= maxCount) {
                break;
            }
        } while (true);
    }

    public static void executeConcurrentSuccessiveDmlWhenDdlRunning(Logger logger, List<Connection> connections,
                                                                    String ddl, AtomicLong jobId, String schemaName,
                                                                    String dml,
                                                                    long timeDelayInMs, Boolean expectedDmlSuccess,
                                                                    String errMsg)
        throws InterruptedException, ExecutionException {
        String useDbSql = "use " + schemaName;

        final int totalThread = connections.size();
        for (Connection connection : connections) {
            Assert.assertNotNull(connection, "connection is null");
        }
        ExecutorService fixedThreadPool = Executors.newFixedThreadPool(totalThread);
        final Lock lock = new ReentrantLock();
        AtomicInteger totalCount = new AtomicInteger(0);
        AtomicBoolean killed = new AtomicBoolean(false);
        int maxCount = 1000;
        List<Future> futures = Lists.newArrayList();
        for (int i = 0; i < totalThread; i++) {
            int finalI = i;
            JdbcUtil.executeQuerySuccess(connections.get(finalI), useDbSql);
            Future<Void> future = (Future<Void>) fixedThreadPool.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        emitDmlConcurrent(logger, lock, connections.get(finalI), finalI, totalCount, killed, maxCount,
                            dml, ddl,
                            jobId,
                            timeDelayInMs, expectedDmlSuccess, errMsg);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            });
            futures.add(future);
        }
        for (int i = 0; i < totalThread; i++) {
            futures.get(i).get();
        }
        fixedThreadPool.shutdown();
    }

    public static void runTestCase(Logger logger, List<Connection> connections, String schemaName, String table,
                                   int connectionNum,
                                   String createTableStmt, String ddl, String dml, Long timeDelayInMs,
                                   Boolean expectedDmlSuccess, String errMsg)
        throws ExecutionException, InterruptedException {
        String useDbSql = "use " + schemaName;
        Assert.assertTrue(connections.size() >= 2 && connections.get(0) != null && connections.get(1) != null);
        Connection connection = connections.get(0);
        JdbcUtil.executeUpdateSuccess(connection, useDbSql);

        ExecutorService fixedThreadPool = Executors.newFixedThreadPool(3);
        AtomicLong jobId = new AtomicLong(-1L);
        Runnable task1 = () -> {
            List<Connection> dmlConnections = connections.subList(1, connectionNum);
            try {
                executeConcurrentSuccessiveDmlWhenDdlRunning(logger, dmlConnections, ddl, jobId, schemaName, dml,
                    timeDelayInMs, expectedDmlSuccess, errMsg);
//                executeConcurrentDml(logger, conn1, schemaName, dml, timeDelayInMs, expectedDmlSuccess, errMsg);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        };
        Runnable task2 = () -> {
            Connection conn1 = connections.get(0);
            try {
                JdbcUtil.executeUpdateSuccess(conn1, useDbSql);
                logger.info(" ddl thread sleep for 1000 ms");
                Thread.sleep(1000);
                logger.info(" ddl thread start to execute " + ddl);
                JdbcUtil.executeUpdateSuccess(conn1, ddl);
            } catch (Exception | AssertionError e) {
                throw new RuntimeException(e);
            }
        };
        Runnable task3 = () -> {
        };
        Future<Void> future1 = (Future<Void>) fixedThreadPool.submit(task1);
        Future<Void> future2 = (Future<Void>) fixedThreadPool.submit(task2);
        Future<Void> future3 = (Future<Void>) fixedThreadPool.submit(task3);
        future1.get();
        try {
            future2.get();
            future3.get();
        } catch (InterruptedException | ExecutionException e) {

        }
        fixedThreadPool.shutdown();
    }
}
