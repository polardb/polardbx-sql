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

package com.alibaba.polardbx.executor.common;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Description: 简单自增序列号生成器
 */
public class AtomicNumberCreator {

    /**
     * int序号自增器，由于复位时需要进行DLC，所以这里是volatile的
     */
    private volatile AtomicInteger integerNumber = new AtomicInteger(0);

    private final ReentrantLock integerLock = new ReentrantLock();

    /**
     * long序号自增器，由于复位时需要进行DLC，所以这里是volatile的
     */
    private volatile AtomicLong longNumber = new AtomicLong(0);

    private final ReentrantLock longLock = new ReentrantLock();

    private AtomicNumberCreator() {
    }

    public static AtomicNumberCreator getNewInstance() {
        return new AtomicNumberCreator();
    }

    /**
     * 生成int的自增数字,从1开始自增，当达到Integer.MAX_VALUE 时会恢复到初始值1
     */
    public int getIntegerNextNumber() {
        int num = integerNumber.incrementAndGet();
        if (num < 0) {
            // 为了保证多线程复位原子性进行DCL双检查锁
            integerLock.lock();
            try {
                if (integerNumber.get() < 0) {
                    // DCL双检查锁通过后对AtomicInteger进行复位
                    integerNumber.set(0);
                }
                return integerNumber.incrementAndGet();
            } finally {
                integerLock.unlock();
            }
        }
        return num;
    }

    /**
     * 生成long的自增数字,从1开始自增，当达到Long.MAX_VALUE 时会恢复到初始值1
     */
    public long getLongNextNumber() {
        long num = longNumber.incrementAndGet();
        // 为了保证多线程复位原子性进行DCL双检查锁
        if (num < 0) {
            longLock.lock();
            try {
                if (longNumber.get() < 0) {
                    // DCL双检查锁通过后对AtomicLong进行复位
                    longNumber.set(0);
                }
                return longNumber.incrementAndGet();
            } finally {
                longLock.unlock();
            }
        }
        return longNumber.get();
    }
}
