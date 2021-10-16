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

package com.alibaba.polardbx.executor.operator.util;

import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MultiProducerConcurrentQueueTest {

    private static final int NUM_THREADS = 4;

    private final Integer END = -1;
    private final MultiProducerConcurrentQueue<Integer> queue = new MultiProducerConcurrentQueue<>(10, END);

    @Test
    public void testNormal() throws Exception {

        Runnable producer = () -> {
            try {
                for (int i = 0; i < 1000; i++) {
                    queue.put(1);
                }
            } catch (InterruptedException e) {
                System.out.println(Thread.currentThread().getName() + ": interrupted");
            } finally {
                queue.putEnd();
            }
        };

        Thread[] threads = new Thread[NUM_THREADS];
        for (int i = 0; i < NUM_THREADS; i++) {
            threads[i] = new Thread(producer);
            threads[i].start();
        }

        int sum = 0;
        int finished = NUM_THREADS;
        while (finished > 0) {
            Integer element = queue.take();
            if (element == END) {
                finished--;
            } else {
                sum += element;
            }
        }

        for (int i = 0; i < NUM_THREADS; i++) {
            threads[i].join();
        }

        assertEquals(sum, 1000 * NUM_THREADS);
    }

    @Test
    public void testInterrupt() throws Exception {

        AtomicInteger actualCount = new AtomicInteger();

        Runnable producer = () -> {
            try {
                for (int i = 0; i < 1000; i++) {
                    queue.put(1);
                    actualCount.addAndGet(1);
                }
            } catch (InterruptedException e) {
                System.out.println(Thread.currentThread().getName() + ": interrupted");
            } finally {
                queue.putEnd();
            }
        };

        Thread[] threads = new Thread[NUM_THREADS];
        for (int i = 0; i < NUM_THREADS; i++) {
            threads[i] = new Thread(producer);
            threads[i].start();
        }

        int sum = 0;
        int finished = NUM_THREADS;
        while (finished > 0) {
            Integer element = queue.take();
            if (element == END) {
                finished--;
            } else {
                sum += element;
            }

            if (sum == 500) {
                for (int i = 0; i < NUM_THREADS; i++) {
                    threads[i].interrupt();
                }
            }
        }

        for (int i = 0; i < NUM_THREADS; i++) {
            threads[i].join();
        }

        assertEquals(sum, actualCount.get());
        assertTrue(sum <= 500 + 10 + NUM_THREADS);
    }
}
