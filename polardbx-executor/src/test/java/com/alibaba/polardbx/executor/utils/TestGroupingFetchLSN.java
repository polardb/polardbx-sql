package com.alibaba.polardbx.executor.utils;

import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlHelper;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

public class TestGroupingFetchLSN {

    private long heartBeat = 0L;

    @Test(expected = TimeoutException.class)
    public void testGrouping1() throws Exception {
        int dnNum = 10;
        int poolSize = 1;
        int fetchTimeout = 1000;
        int mockFetchLSN = 100;
        testGrouping(dnNum, poolSize, fetchTimeout, mockFetchLSN);
    }

    @Test(expected = TimeoutException.class)
    public void testGrouping2() throws Exception {
        int dnNum = 10;
        int poolSize = 4;
        int fetchTimeout = 1000;

        int mockFetchLSN = 300;
        testGrouping(dnNum, poolSize, fetchTimeout, mockFetchLSN);
    }

    @Test
    public void testGrouping3() throws Exception {
        int dnNum = 10;
        int poolSize = 4;
        int fetchTimeout = 2000;

        int mockFetchLSN = 300;
        testGrouping(dnNum, poolSize, fetchTimeout, mockFetchLSN);
    }

    @Test
    public void testGrouping4() throws Exception {
        int dnNum = 10;
        int poolSize = 10;
        int fetchTimeout = 1000;

        int mockFetchLSN = 300;
        testGrouping(dnNum, poolSize, fetchTimeout, mockFetchLSN);
    }

    @Test
    public void testGrouping5() throws Exception {
        int dnNum = 24;
        int poolSize = 4;
        int fetchTimeout = 1000;

        int mockFetchLSN = 10;

        testGrouping(dnNum, poolSize, fetchTimeout, mockFetchLSN);
    }

    public void testGrouping(
        int dnNum, int poolSize, int fetchTimeout, int mockFetchLSN) throws Exception {
        AtomicLong iterNum = new AtomicLong(5000);
        int sessionNum = 1024;
        GroupingFetchLSN fetchLSN = Mockito.spy(GroupingFetchLSN.getInstance());
        fetchLSN.resetGroupingPool(fetchTimeout, poolSize);
        when(fetchLSN.getLsnBasedCDC(anyString(), anyLong(), anyString())).thenAnswer(new Answer<Long>() {
            @Override
            public Long answer(InvocationOnMock invocationOnMock) throws Throwable {
                DdlHelper.waitToContinue(mockFetchLSN);
                return 100L;
            }
        });

        AtomicReference<Exception> throwable = new AtomicReference<>();
        Thread[] threads = new Thread[sessionNum];
        for (int i = 0; i < sessionNum; i++) {
            final int key = i % dnNum;
            Thread t = new Thread(() -> {

                while (true) {
                    long value = iterNum.decrementAndGet();
                    if (value <= 0) {
                        break;
                    }
                    try {
                        fetchLSN.groupingLsn(key + "", heartBeat++);
                    } catch (Exception t1) {
                        throwable.set(t1);
                        break;
                    }
                }
            });
            threads[i] = t;
        }
        for (int i = 0; i < sessionNum; i++) {
            threads[i].start();
        }

        for (int i = 0; i < sessionNum; i++) {
            threads[i].join();
        }
        if (throwable.get() != null) {
            throw throwable.get();
        }
    }
}
