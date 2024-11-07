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

package com.alibaba.polardbx.qatest.cdc;

import com.alibaba.polardbx.qatest.util.JdbcUtil;
import lombok.extern.slf4j.Slf4j;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by ziyang.lb
 */
@Slf4j
public class Bank {

    public static final int ACCOUNT_COUNT = 10000;
    public static final int ACCOUNT_INIT_AMOUNT = 1000;

    private final int accountCount;
    private final DataSource dataSource;
    private final AtomicLong nextSequence = new AtomicLong(0);
    private final Random random = new Random(System.currentTimeMillis());

    public Bank(int accountCount, DataSource dataSource) {
        this.accountCount = accountCount;
        this.dataSource = dataSource;
    }

    public void run(int parallelism) {
        ExecutorService executor = Executors.newFixedThreadPool(parallelism, Thread::new);

        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < parallelism; i++) {
            Future<?> future = executor.submit(() -> {
                log.info("start to do transfer.");
                int count = 0;
                int errorCount = 0;

                while (count < 1000) {
                    Transfer transfer = roundTransfer();
                    try {
                        doTransfer(transfer);
                        count++;
                    } catch (SQLException e) {
                        errorCount++;
                        if (errorCount > 100) {
                            throw new RuntimeException("transfer error.", e);
                        }
                    }
                }
            });
            futures.add(future);
        }

        futures.forEach(f -> {
            try {
                f.get();
            } catch (Throwable e) {
                throw new RuntimeException("something goes wrong with transfer");
            }
        });
    }

    private void updateBalance(int src, int amount, Connection connection) throws SQLException {
        PreparedStatement ps = connection.prepareStatement("UPDATE accounts SET balance = balance + ? where id = ?");
        ps.setInt(1, amount);
        ps.setInt(2, src);
        ps.executeUpdate();
        ps.close();
    }

    private int round(int accountCount) {
        return (int) nextSequence.addAndGet(random.nextInt(9) + 1) % accountCount;
    }

    private Transfer roundTransfer() {
        int src, dst;
        src = round(accountCount);
        do {
            dst = round(accountCount);
        } while (src == dst);
        return new Transfer(src, dst, new Random(System.currentTimeMillis()).nextInt(100));
    }

    private void doTransfer(Transfer transfer) throws SQLException {
        Connection conn = null;
        try {
            conn = dataSource.getConnection();
            conn.setAutoCommit(false);
            updateBalance(transfer.getSrc(), -transfer.getAmount(), conn);
            updateBalance(transfer.getDst(), transfer.getAmount(), conn);
            conn.commit();
            Thread.sleep(5);
        } catch (Exception e) {
            conn.rollback();
        } finally {
            JdbcUtil.closeConnection(conn);
        }
    }

    static class Transfer {

        private final int src;
        private final int dst;
        private final int amount;

        public Transfer(int src, int dst, int amount) {
            this.src = src;
            this.dst = dst;
            this.amount = amount;
        }

        public int getSrc() {
            return src;
        }

        public int getDst() {
            return dst;
        }

        public int getAmount() {
            return amount;
        }

        @Override
        public String toString() {
            return "Transfer{" + "src=" + src + ", dst=" + dst + ", amount=" + amount + '}';
        }
    }

    public static void main(String args[]) {
        System.out.println(new Random(System.currentTimeMillis()).nextInt(100));
    }
}
