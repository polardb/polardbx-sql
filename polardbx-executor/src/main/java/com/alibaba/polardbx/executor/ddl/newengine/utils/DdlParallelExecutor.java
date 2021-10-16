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

package com.alibaba.polardbx.executor.ddl.newengine.utils;

import com.alibaba.polardbx.common.async.AsyncCallableTask;
import com.alibaba.polardbx.common.async.AsyncTask;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;

/**
 * Refer to ExecutorTemplate and add simple parallel control.
 * Note that this is not thread-safe.
 */
public class DdlParallelExecutor<T> {

    private final ExecutorCompletionService completionService;
    private final List<Future> futures;

    private final int parallelism;
    private volatile int parallelCount = 0;

    public DdlParallelExecutor(Executor executor, int parallelism) {
        this.completionService = new ExecutorCompletionService(executor);
        this.futures = Collections.synchronizedList(new ArrayList<>());
        this.parallelism = parallelism;
    }

    public void submit(Callable<T> task) {
        Future<T> future = completionService.submit(AsyncCallableTask.build(task));
        futures.add(future);
        check(future);
    }

    public void submit(Runnable task) {
        Future<T> future = completionService.submit(AsyncTask.build(task), null);
        futures.add(future);
        check(future);
    }

    public boolean hasCapacity() {
        return parallelCount < parallelism;
    }

    private void check(Future<T> future) {
        if (future.isDone()) {
            try {
                future.get();
            } catch (InterruptedException e) {
                cancel();
                throw new RuntimeException(e);
            } catch (Throwable e) {
                cancel();
                throw new RuntimeException(e);
            }
        } else {
            parallelCount++;
        }
    }

    public synchronized List<T> waitForResult() {
        List<T> result = new ArrayList();
        RuntimeException exception = null;

        for (int i = 0; i < futures.size(); i++) {
            try {
                Future<T> future = completionService.take();
                result.add(future.get());
                parallelCount--;
            } catch (InterruptedException e) {
                exception = new RuntimeException(e);
                break;
            } catch (Throwable e) {
                exception = new RuntimeException(e);
                break;
            }
        }

        if (exception != null) {
            cancel();
            throw exception;
        } else {
            return result;
        }
    }

    public synchronized T pollResult() {
        RuntimeException exception = null;

        try {
            Future<T> future = completionService.poll();
            if (future != null) {
                parallelCount--;
                futures.remove(future);
                return future.get();
            }
        } catch (Throwable e) {
            exception = new RuntimeException(e);
        }

        if (exception != null) {
            cancel();
            throw exception;
        }

        return null;
    }

    public void cancel() {
        for (int i = 0; i < futures.size(); i++) {
            Future<T> future = futures.get(i);
            if (!future.isDone() && !future.isCancelled()) {
                future.cancel(true);
                parallelCount--;
            }
        }
    }

    public void clear() {
        futures.clear();
        parallelCount = 0;
    }
}
