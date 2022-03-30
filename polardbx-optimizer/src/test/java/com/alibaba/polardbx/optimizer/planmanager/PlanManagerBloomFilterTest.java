package com.alibaba.polardbx.optimizer.planmanager;

import com.alibaba.polardbx.common.utils.bloomfilter.BloomFilter;
import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;

public class PlanManagerBloomFilterTest {

    private final String SQL_PARAMETRIZED1 = "select * from test where id = ?";
    private final String SQL_PARAMETRIZED2 = "select * from test where id = ? order by name";
    private final String SQL_PARAMETRIZED3 = "select * from test where id between ? and ? order by name";

    private final List<String> SQL_LIST =  ImmutableList.of(SQL_PARAMETRIZED1, SQL_PARAMETRIZED2, SQL_PARAMETRIZED3);

    private BloomFilter sqlHistoryBloomfilter = BloomFilter.createEmpty(1000000, 0.05);

    @Test
    public void recordSqlConcurrentlyTest() {
        final int threadCnt = 5;
        final int putCnt = 10;
        final CountDownLatch countDownLatch = new CountDownLatch(threadCnt);
        for (int i = 0; i < threadCnt; i++) {
            Thread t = new Thread(() -> {
                try {
                    for (int i1 = 0; i1 < putCnt; i1++) {
                        String sql = SQL_LIST.get(i1 % SQL_LIST.size());
                        sqlHistoryBloomfilter.putSynchronized(sql);
                        Assert.assertTrue(sqlHistoryBloomfilter.mightContainSynchronized(sql));
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    Assert.fail(e.getMessage());
                } finally {
                    countDownLatch.countDown();
                }
            });
            t.start();
        }
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
        for (int i = 0; i < threadCnt; i++) {
            Thread t = new Thread(() -> {
                for (String sql : SQL_LIST) {
                    try {
                        Assert.assertTrue(sqlHistoryBloomfilter.mightContainSynchronized(sql));
                    } catch (Exception e) {
                        e.printStackTrace();
                        Assert.fail(e.getMessage());
                    }
                }
            });
            t.start();
        }
    }
}
