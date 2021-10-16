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

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;

/**
 * Multi-Producer Concurrent Queue with back-pressure and EOF support
 *
 */
public class MultiProducerConcurrentQueue<E> {

    private final int capacity;
    private final Semaphore semaphore;
    private final LinkedBlockingQueue<E> buffer;
    private final E endMarker;

    public MultiProducerConcurrentQueue(int capacity, E end) {
        this.capacity = capacity;
        this.semaphore = new Semaphore(capacity);
        this.buffer = new LinkedBlockingQueue<>();
        this.endMarker = end;
    }

    /**
     * Put a regular element into queue. If queue is full, wait until some space available
     */
    public void put(E element) throws InterruptedException {
        assert element != endMarker;
        semaphore.acquire();
        buffer.add(element);
    }

    /**
     * Put an end-marker into queue, which does not count in capacity limit
     */
    public void putEnd() {
        buffer.add(endMarker);
    }

    /**
     * Take an element from queue. Note that the returned element may be end-marker
     */
    public E take() throws InterruptedException {
        E element = buffer.take();
        if (element != endMarker) {
            semaphore.release();
        }
        return element;
    }

    public E poll() {
        E element = buffer.poll();
        if (element != endMarker) {
            semaphore.release();
        }
        return element;
    }

    /**
     * Check whether the taken element is an end-marker
     */
    public boolean isEnd(E element) {
        return element == endMarker;
    }

    /**
     * Number of elements in the queue (except end-marker)
     */
    public int size() {
        return capacity - semaphore.availablePermits();
    }
}
