package com.alibaba.polardbx.qatest.ddl.auto.omc;

import com.alibaba.polardbx.qatest.ddl.auto.movepartition.MovePartitionDmlBaseTest;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Before;
import org.junit.Test;

import java.security.SecureRandom;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public class UpsertWithHotValueTest extends MovePartitionDmlBaseTest {
    static private final String DATABASE_NAME = "UpsertWithHotValueOmc";

    private static final String createTableSql = "CREATE TABLE `test_upsert_with_hot_value` ( "
        + " `id` bigint UNSIGNED NOT NULL AUTO_INCREMENT, "
        + " `unique_col` varchar(64) NOT NULL, "
        + " `update_time` bigint NOT NULL, "
        + " `var` bigint NOT NULL, "
        + " PRIMARY KEY (`id`), "
        + " UNIQUE KEY `uk` (`unique_col`) "
        + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 DEFAULT COLLATE = utf8mb4_bin"
        + " PARTITION BY KEY(`unique_col`) partitions 3;";

    private static final String upsertSql =
        "insert into test_upsert_with_hot_value(unique_col, update_time, var) values(?, ?, ?) ON DUPLICATE KEY UPDATE `update_time` = values(`update_time`), `var` = values(`var`)";

    private static final int batchSize = 1024;

    public UpsertWithHotValueTest() {
        super(DATABASE_NAME);
    }

    @Before
    public void before() {
        doReCreateDatabase();

        JdbcUtil.executeUpdateSuccess(mysqlConnection, "DROP TABLE IF EXISTS `test_upsert_with_hot_value`");
    }

    @Test
    public void testUpsertWithHotValue() {
        // create table
        JdbcUtil.executeSuccess(tddlConnection, createTableSql);

        // start upsert
        AtomicBoolean stop = new AtomicBoolean(false);
        List<Future> futures = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            futures.add(dmlPool.submit(new UpsertTask(stop, throwException, 100)));
        }
        logger.info("start upsert");

        // sleep 3 seconds
        try {
            TimeUnit.SECONDS.sleep(3);
        } catch (InterruptedException e) {
            // ignore
        }

        // do omc
        logger.info("omc start");
        JdbcUtil.executeSuccess(tddlConnection,
            "alter table test_upsert_with_hot_value modify column var bigint NULL, algorithm=omc");
        logger.info("omc finished");

        // stop upsert
        stop.set(true);
        for (Future future : futures) {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public final ExecutorService dmlPool = Executors.newFixedThreadPool(10);

    static class UpsertTask implements Runnable {
        private final String allChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        private final int length = 44;
        private final int cacheSize = 10000;
        private final double repeatProbability = 0.8;
        private List<String> history = new ArrayList<>();
        private final SecureRandom random = new SecureRandom();

        private final AtomicBoolean stop;
        private final Consumer<Exception> errHandler;
        private final int maxSeconds;

        public UpsertTask(AtomicBoolean stop, Consumer<Exception> errHandler, int maxSeconds) {
            this.stop = stop;
            this.errHandler = errHandler;
            this.maxSeconds = maxSeconds;
        }

        @Override
        public void run() {
            final long startTime = System.currentTimeMillis();
            long threadId = Thread.currentThread().getId();
            int count = 0;
            do {
                try (Connection conn = ConnectionManager.getInstance().newPolarDBXConnection()) {
                    JdbcUtil.executeSuccess(conn, "use " + DATABASE_NAME);
                    try (PreparedStatement pstmt = conn.prepareStatement(upsertSql)) {
                        Random random = new Random();
                        for (int i = 0; i < batchSize; i++) {
                            pstmt.setString(1, threadId + generateRandomString());
                            pstmt.setLong(2, System.currentTimeMillis());
                            pstmt.setLong(3, random.nextInt(256));

                            pstmt.addBatch();
                        }

                        pstmt.executeBatch();
                        count++;
                        logger.info("已执行 " + count + " 轮 " + Thread.currentThread());
                    }
                } catch (Exception e) {
                    if (notIgnoredErrors(e)) {
                        errHandler.accept(e);
                    }
                }

                if (System.currentTimeMillis() - startTime > maxSeconds * 1000L) {
                    break; // 100s timeout
                }
            } while (!stop.get());

            System.out.println(
                Thread.currentThread().getName() + " quit after " + count * batchSize + " records inserted");
        }

        public String generateRandomString() {
            // 以指定概率决定是否从缓存中取
            if (random.nextDouble() < repeatProbability && !history.isEmpty()) {
                return getRandomFromHistory();
            } else {
                String newStr = generateUniqueRandomString();
                addToHistory(newStr);
                return newStr;
            }
        }

        // 生成一个全新的随机字符串
        private String generateUniqueRandomString() {
            StringBuilder sb = new StringBuilder(length);
            for (int i = 0; i < length; i++) {
                int index = random.nextInt(allChars.length());
                sb.append(allChars.charAt(index));
            }
            return sb.toString();
        }

        // 从历史记录中随机选一个
        private String getRandomFromHistory() {
            int index = random.nextInt(history.size());
            return history.get(index);
        }

        // 添加到历史缓存，并保持缓存不超过设定大小
        private void addToHistory(String str) {
            if (history.contains(str)) {
                return; // 如果已经存在，不重复添加
            }
            if (history.size() > cacheSize) {
                return;
            }
            history.add(str);
        }
    }
}
