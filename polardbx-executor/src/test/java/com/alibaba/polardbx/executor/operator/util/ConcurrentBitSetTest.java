package com.alibaba.polardbx.executor.operator.util;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ConcurrentBitSetTest {
    private static final int SIZE = 128;
    private ConcurrentBitSetImpl bitSet;

    @Test
    public void test() {
        ConcurrentBitSet bitSet = new ConcurrentBitSetImpl(SIZE);

        bitSet.set(0);
        bitSet.set(1);
        bitSet.set(3);

        int clearBitIndex = bitSet.nextClearBit(0);
        Assert.assertEquals("First clear bit index from 0: " + clearBitIndex, 2, clearBitIndex); // 输出: 2

        clearBitIndex = bitSet.nextClearBit(2);
        Assert.assertEquals("First clear bit index from 2: " + clearBitIndex, 2, clearBitIndex); // 输出: 2

        clearBitIndex = bitSet.nextClearBit(3);
        Assert.assertEquals("First clear bit index from 3: " + clearBitIndex, 4, clearBitIndex); // 输出: 4

        clearBitIndex = bitSet.nextClearBit(4);
        Assert.assertEquals("First clear bit index from 4: " + clearBitIndex, 4, clearBitIndex); // 输出: 4

        clearBitIndex = bitSet.nextClearBit(5);
        Assert.assertEquals("First clear bit index from 5: " + clearBitIndex, 5, clearBitIndex); // 输出: 5
    }

    @Before
    public void setUp() {
        bitSet = new ConcurrentBitSetImpl(SIZE);
    }

    @Test
    public void testSetGetClear() {
        bitSet.set(5);
        assertTrue(bitSet.get(5));

        bitSet.clear(5);
        assertFalse(bitSet.get(5));
    }

    @Test
    public void testConcurrentSetAndGet() throws InterruptedException {
        int numThreads = 10;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch latch = new CountDownLatch(numThreads);
        AtomicInteger totalSetBits = new AtomicInteger(0);

        // 启动线程并设置不同的bit
        for (int i = 0; i < numThreads; i++) {
            final int bitIndex = i;
            executor.submit(() -> {
                bitSet.set(bitIndex);
                totalSetBits.incrementAndGet();
                latch.countDown();
            });
        }

        latch.await();
        executor.shutdown();

        for (int i = 0; i < numThreads; i++) {
            assertTrue(bitSet.get(i));
        }

        assertEquals(numThreads, totalSetBits.get());
    }

    @Test
    public void testConcurrentSetAndClear() throws InterruptedException {
        int numThreads = 10;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch latch = new CountDownLatch(numThreads);

        for (int i = 0; i < numThreads; i++) {
            final int bitIndex = i;
            executor.submit(() -> {
                bitSet.set(bitIndex);
                bitSet.clear(bitIndex);
                latch.countDown();
            });
        }

        latch.await();
        executor.shutdown();

        for (int i = 0; i < numThreads; i++) {
            assertFalse(bitSet.get(i));
        }
    }

    @Test
    public void testNextClearBit() throws InterruptedException {
        int numThreads = 10;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        CountDownLatch latch = new CountDownLatch(numThreads);

        for (int i = 0; i < numThreads; i++) {
            final int bitIndex = i;
            executor.submit(() -> {
                bitSet.set(bitIndex);
                latch.countDown();
            });
        }

        latch.await();
        executor.shutdown();

        for (int i = 0; i < numThreads; i++) {
            int nextClearBit = bitSet.nextClearBit(i);
            assertEquals(numThreads, nextClearBit);
        }
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testSetWithOutOfBoundsIndex() {
        bitSet.set(SIZE);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testSetWithOutOfBoundsNegativeIndex() {
        bitSet.set(-1);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testClearWithOutOfBoundsIndex() {
        bitSet.clear(SIZE);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testClearWithOutOfBoundsNegativeIndex() {
        bitSet.clear(-1);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testGetWithOutOfBoundsIndex() {
        bitSet.get(SIZE);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testGetWithOutOfBoundsNegativeIndex() {
        bitSet.get(-1);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testNextClearBitWithOutOfBoundsIndex() {
        bitSet.nextClearBit(SIZE);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testNextClearBitWithOutOfBoundsNegativeIndex() {
        bitSet.nextClearBit(-1);
    }
}