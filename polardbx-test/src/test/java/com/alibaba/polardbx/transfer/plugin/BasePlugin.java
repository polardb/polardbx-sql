package com.alibaba.polardbx.transfer.plugin;

import com.alibaba.polardbx.transfer.config.TomlConfig;
import com.alibaba.polardbx.transfer.utils.Account;
import com.alibaba.polardbx.transfer.utils.Utils;
import com.moandjiezana.toml.Toml;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

/**
 * @author wuzhe
 */
public abstract class BasePlugin implements IPlugin {
    private static final Logger logger = LoggerFactory.getLogger(BasePlugin.class);
    // set it to true and invoke FAIL.notifyAll() to terminate the test runner immediately
    private static final AtomicBoolean FAIL = new AtomicBoolean(false);

    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (Throwable t) {
            // ignore
        }
    }

    protected final String dsn;
    protected final String props;
    protected final int rowCount;
    protected final long initBalance;
    private final AtomicLong ops = new AtomicLong(0);
    private final ConcurrentHashMap<String, ConnectionPool> connPoolMap = new ConcurrentHashMap<>();
    protected Worker worker;
    protected boolean enabled = false;
    protected int threads;

    protected BasePlugin() {
        Toml config = TomlConfig.getConfig();
        dsn = config.getString("dsn");
        props = config.getString("conn_properties");
        rowCount = Math.toIntExact(config.getLong("row_count"));
        initBalance = config.getLong("initial_balance", 100L);
        assert dsn != null;
        assert rowCount > 0;
    }

    public static void waitUtilTimeout(long timeout) throws InterruptedException {
        synchronized (FAIL) {
            if (!FAIL.get()) {
                FAIL.wait(timeout);
            }
        }
    }

    private static void fail() {
        synchronized (FAIL) {
            FAIL.set(true);
            FAIL.notifyAll();
        }
    }

    @Override
    public long getOpAndClear() {
        return ops.getAndSet(0);
    }

    /**
     * Define your test case in this method.
     * Your case will be run repeatedly until runner timeout or interrupted.
     * Typically, you can invoke getConnection(dsn) to get a connection, execute some SQLs, and check state.
     */
    protected abstract void runInternal();

    @Override
    public final void run() {
        if (!enabled) {
            return;
        }

        String name = this.getClass().getSimpleName();
        logger.info("Ready to run " + name);
        if (threads <= 0) {
            threads = 1;
        }
        worker = new Worker(threads, name);
        worker.run(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                runInternal();
                finishOp();
            }
        });
    }

    @Override
    public boolean isEnabled() {
        return enabled;
    }

    @Override
    public void interrupt() {
        if (null != worker) {
            worker.interrupt();
        }
        for (ConnectionPool pool : connPoolMap.values()) {
            pool.clear();
        }
    }

    /**
     * Usage:
     * <pre>{@code
     * getConnectionAndExecute(dsn, (conn, error) -> {
     *     try (Statement stmt = conn.createStatement() {
     *         stmt.execute(your_sql);
     *     } catch(SQLException e) {
     *         error.set(e);
     *         // your error handle code
     *     }
     * });
     * }</pre>
     * Connection will be discarded if error occurs, or put back to connection pool if anything is ok.
     */
    protected void getConnectionAndExecute(String dsn,
                                           BiConsumer<MyConnection, AtomicReference<SQLException>> consumer) {
        MyConnection conn = null;
        AtomicReference<SQLException> error = new AtomicReference<>();
        try {
            conn = getConnection(dsn);
            consumer.accept(conn, error);
            if (null != error.get()) {
                throw error.get();
            }
        } catch (SQLException e) {
            if (null != conn) {
                try {
                    conn.discard();
                } catch (Throwable t) {
                    // ignore
                }
                conn = null;
            }
        } finally {
            if (null != conn) {
                conn.close();
            }
        }
    }

    protected MyConnection getConnection(String dsn) throws SQLException {
        ConnectionPool pool = connPoolMap.computeIfAbsent(dsn, k -> new ConnectionPool(dsn, props));
        return pool.get();
    }

    protected void finishOp() {
        ops.incrementAndGet();
    }

    protected void checkConsistency(List<Account> accounts, String hint) {
        if (null == accounts) {
            return;
        }
        long expectTotal = rowCount * initBalance;
        long actualTotal = 0;
        for (Account account : accounts) {
            actualTotal += account.balance;
        }
        if (actualTotal != expectTotal) {
            error1(accounts, hint, "Total balance not matches, expect: " + expectTotal + ", actual: " + actualTotal);
        }
    }

    protected void checkStrongConsistency(List<Account> masterAccounts, List<Account> slaveAccounts, String hint) {
        if (null == masterAccounts || null == slaveAccounts) {
            return;
        }
        if (masterAccounts.size() != slaveAccounts.size()) {
            error2(masterAccounts, slaveAccounts, hint,
                "Master and slave size not matches, master: " + masterAccounts.size() + ", slave: "
                    + slaveAccounts.size());
            return;
        }
        Collections.sort(masterAccounts);
        Collections.sort(slaveAccounts);
        for (int i = 0; i < masterAccounts.size(); i++) {
            Account master = masterAccounts.get(i);
            Account slave = slaveAccounts.get(i);
            if (master.id != slave.id) {
                error2(masterAccounts, slaveAccounts, hint,
                    "Master and slave id not matches, master: " + master.id + ", slave: " + slave.id);
            }
            if (master.version > slave.version) {
                error2(masterAccounts, slaveAccounts, hint,
                    "Master is fresher than slave, master: " + master.version + ", slave: " + slave.version);
            }
        }
    }

    private void error1(List<Account> accounts, String hint, String errorMsg) {
        if (Thread.currentThread().isInterrupted()) {
            // already interrupt
            return;
        }
        generateErrorMsg(accounts, hint);
        fail();
        throw new RuntimeException(errorMsg);
    }

    private void error2(List<Account> masterAccounts, List<Account> slaveAccounts, String hint, String errorMsg) {
        if (Thread.currentThread().isInterrupted()) {
            // already interrupt
            return;
        }
        generateErrorMsg(masterAccounts, "master: " + hint);
        generateErrorMsg(slaveAccounts, "slave: " + hint);
        fail();
        throw new RuntimeException(errorMsg);
    }

    public void generateErrorMsg(List<Account> accounts, String hint) {
        Collections.sort(accounts);
        StringBuilder sb = new StringBuilder("Inconsistency found: ").append("\n");
        for (Account account : accounts) {
            sb.append(account.id).append(":").append(account.balance).append("\n");
        }
        sb.append("trace hint ").append(hint);
        logger.error(sb.toString());
    }

    protected void errorAllTypesTest1(List<String> checkResults, String errorMsg) {
        if (Thread.currentThread().isInterrupted()) {
            // already interrupt
            return;
        }
        StringBuilder sb = new StringBuilder("All types test failed: ").append("\n");
        for (String checkResult : checkResults) {
            sb.append(checkResult).append("\n");
        }
        fail();
        throw new RuntimeException(errorMsg);
    }

    public static boolean success() {
        return !FAIL.get();
    }

    private static class Worker {
        private final ExecutorService executor;
        private final int threads;

        public Worker(int threads, String name) {
            this.threads = threads;
            ThreadFactory threadFactory = new MyThreadFactory(name);
            executor = Executors.newFixedThreadPool(threads, threadFactory);
        }

        public void run(Runnable task) {
            for (int i = 0; i < threads; i++) {
                executor.execute(task);
            }

            executor.shutdown();
        }

        public void interrupt() {
            executor.shutdownNow();
        }

        static class MyThreadFactory implements ThreadFactory {
            private final String threadNamePrefix;

            public MyThreadFactory(String threadNamePrefix) {
                this.threadNamePrefix = threadNamePrefix;
            }

            @Override
            public Thread newThread(Runnable r) {
                Thread thread = Executors.defaultThreadFactory().newThread(r);
                thread.setName(threadNamePrefix + "-" + thread.getId());
                return thread;
            }
        }
    }

    private static class ConnectionPool {
        private final String dsn;
        private final String props;
        ConcurrentLinkedQueue<MyConnection> connections = new ConcurrentLinkedQueue<>();

        public ConnectionPool(String dsn, String props) {
            this.dsn = dsn;
            this.props = props;
        }

        public void add(MyConnection conn) {
            connections.offer(conn);
        }

        public MyConnection get() throws SQLException {
            MyConnection conn;
            if (null == (conn = connections.poll())) {
                // poll is empty, create new connection
                conn = new MyConnection(this, dsn, props);
            }
            return conn;
        }

        public void clear() {
            for (MyConnection connection : connections) {
                try {
                    connection.discard();
                } catch (Throwable ignored) {

                }
            }
            connections.clear();
        }

    }

    public static class MyConnection implements Closeable {
        private final ConnectionPool pool;
        private final Connection conn;
        private final String dsn;
        private boolean discarded = false;

        public MyConnection(ConnectionPool pool, String dsn, String props) throws SQLException {
            this.pool = pool;
            this.dsn = dsn;
            this.conn = Utils.getConnection(dsn, props);
        }

        @Override
        public void close() {
            if (!discarded) {
                pool.add(this);
            }
        }

        public void discard() throws SQLException {
            discarded = true;
            conn.close();
        }

        public Statement createStatement() throws SQLException {
            return conn.createStatement();
        }

        public void setAutoCommit(boolean b) throws SQLException {
            conn.setAutoCommit(b);
        }

        public void setReadOnly(boolean b) throws SQLException {
            conn.setReadOnly(b);
        }

        public void rollback() throws SQLException {
            conn.rollback();
        }
    }
}
