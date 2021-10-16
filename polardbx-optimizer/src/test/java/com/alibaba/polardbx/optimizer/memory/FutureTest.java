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

package com.alibaba.polardbx.optimizer.memory;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.thread.NamedThreadFactory;
import org.junit.Test;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

public class FutureTest {
    @Test
    public void testFutureWithTimeout() throws Exception {
        ListenableFuture<?> listenableFuture = SettableFuture.create();
        ScheduledExecutorService
            executorService = Executors.newScheduledThreadPool(1, new NamedThreadFactory("FutureTest", true));
        ListenableFuture<?> delege = Futures.withTimeout(listenableFuture, 1, TimeUnit.SECONDS, executorService);
        Assert.assertTrue(!delege.isDone());
        Thread.sleep(1100);
        Assert.assertTrue(delege.isDone());
    }

    @Test
    public void testFuture1() {
        SettableFuture<?> testFuture = SettableFuture.create();
        Assert.assertTrue(!testFuture.isDone());
        SettableFuture<?> listenableFuture = SettableFuture.create();
        listenableFuture.set(null);
        Futures.addCallback(listenableFuture, new FutureCallback<Object>() {
            @Override
            public void onSuccess(Object result) {
                testFuture.set(null);
            }

            @Override
            public void onFailure(Throwable t) {
                testFuture.set(null);
            }
        }, directExecutor());
        Assert.assertTrue(testFuture.isDone());
    }

    @Test
    public void testFuture2() throws Exception {
        SettableFuture<?> testFuture = SettableFuture.create();
        SettableFuture<?> listenableFuture = SettableFuture.create();
        Futures.addCallback(listenableFuture, new FutureCallback<Object>() {
            @Override
            public void onSuccess(Object result) {
                testFuture.set(null);
            }

            @Override
            public void onFailure(Throwable t) {
                testFuture.set(null);
            }
        }, directExecutor());

        Thread.sleep(10);
        Thread thread = new Thread(new Runnable() {

            public void run() {
                listenableFuture.set(null);
            }
        });
        thread.start();
        thread.join();
        Assert.assertTrue(testFuture.isDone());
    }
}
