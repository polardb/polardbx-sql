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

package com.alibaba.polardbx.common.utils.thread;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;


public class ExecutorTemplate {

    private volatile ExecutorCompletionService completionService = null;
    private volatile List<Future> futures = null;

    public ExecutorTemplate(Executor executor) {
        completionService = new ExecutorCompletionService(executor);
        futures = Collections.synchronizedList(new ArrayList<Future>());
    }

    public void submit(Callable<Exception> task) {
        Future future = completionService.submit(task);
        futures.add(future);
        check(future);
    }

    public void submit(Runnable task) {
        Future future = completionService.submit(task, null);
        futures.add(future);
        check(future);
    }

    private void check(Future future) {
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
        }
    }

    public synchronized List<?> waitForResult() {
        List result = new ArrayList();
        RuntimeException exception = null;

        int index = 0;
        while (index < futures.size()) {
            try {
                Future future = completionService.take();
                result.add(future.get());
            } catch (InterruptedException e) {
                exception = new RuntimeException(e);
                break;
            } catch (Throwable e) {
                exception = new RuntimeException(e);
                break;
            }

            index++;
        }

        if (exception != null) {

            cancel();
            throw exception;
        } else {
            return result;
        }
    }

    public void cancel() {
        for (int i = 0; i < futures.size(); i++) {
            Future future = futures.get(i);
            if (!future.isDone() && !future.isCancelled()) {
                future.cancel(true);
            }
        }
    }

    public void clear() {
        futures.clear();
    }
}
