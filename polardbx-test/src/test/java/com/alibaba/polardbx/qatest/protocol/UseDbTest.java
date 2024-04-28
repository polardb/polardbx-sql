package com.alibaba.polardbx.qatest.protocol;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import net.jcip.annotations.NotThreadSafe;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.alibaba.polardbx.qatest.BaseSequenceTestCase.quoteSpecialName;

@NotThreadSafe
public class UseDbTest extends ReadBaseTestCase {

    private static final String TABLE_NAME = "xrpc_use_db";

    private static final String CREATE_TABLE = "create table " + quoteSpecialName(TABLE_NAME) + " (\n"
        + "  `x` int(11) NOT NULL AUTO_INCREMENT,\n"
        + "  `y` int(11) NOT NULL,\n"
        + "  PRIMARY KEY (`x`)\n"
        + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4  dbpartition by hash(`x`) tbpartition by hash(`x`) tbpartitions 3";

    @Before
    public void initData() {
        if (usingNewPartDb()) {
            return;
        }

        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "drop table if exists " + quoteSpecialName(TABLE_NAME));
        JdbcUtil.executeUpdateSuccess(tddlConnection, CREATE_TABLE);

        // insert 400 data
        for (int i = 0; i < 10; ++i) {
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "insert into " + quoteSpecialName(TABLE_NAME)
                    + " (y) values (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (0), (1), (2), (3), (4), (5), (6), (7), (8), (9)");
        }
    }

    @Test
    public void checkUseDbWithMultiThreads() throws Exception {
        final int thread_number = 10;
        final int run_seconds = 10;

        final AtomicBoolean exit = new AtomicBoolean(false);
        final AtomicReference<Throwable> error = new AtomicReference<>(null);
        final List<Thread> threads = new ArrayList<>(thread_number);
        for (int i = 0; i < thread_number; ++i) {
            final Thread thread = new Thread(() -> {
                while (!exit.get()) {
                    try (Connection conn = getPolardbxConnection()) {
                        for (int j = 0; j < 20; ++j) {
                            if (exit.get()) {
                                break;
                            }

                            // check with local executor and streaming fast stop
                            JdbcUtil.executeQuerySuccess(conn,
                                "select * from " + quoteSpecialName(TABLE_NAME) + "limit 1");
                        }
                    } catch (Throwable e) {
                        error.compareAndSet(null, e);
                    }
                }
            });
            thread.start();
            threads.add(thread);
        }

        for (int i = 0; i < run_seconds; ++i) {
            if (null == error.get()) {
                Thread.sleep(1000);
            } else {
                break;
            }
        }

        exit.set(true);
        for (Thread t : threads) {
            t.join();
        }

        if (error.get() != null) {
            error.get().printStackTrace();
            Assert.fail(error.get().getMessage());
        }
    }
}
