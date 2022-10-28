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

package com.alibaba.polardbx.transaction.utils;

import com.alibaba.polardbx.common.constants.TransactionAttribute;
import com.alibaba.polardbx.common.utils.AsyncUtils;
import com.alibaba.polardbx.transaction.async.AsyncTaskQueue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

public class TransactionAsyncUtils {

    public static void runTasksConcurrently(AsyncTaskQueue asyncQueue, List<Runnable> tasks) {
        if (tasks.isEmpty()) {
            return;
        }
        if (tasks.size() >= TransactionAttribute.CONCURRENT_COMMIT_LIMIT) {
            // Execute actions (except the last one) in async queue concurrently
            List<Future> futures = new ArrayList<>(tasks.size() - 1);
            for (int i = 0; i < tasks.size() - 1; i++) {
                futures.add(asyncQueue.submit(tasks.get(i)));
            }

            // Execute the last action by this thread
            RuntimeException exception = null;
            try {
                tasks.get(tasks.size() - 1).run();
            } catch (RuntimeException ex) {
                exception = ex;
            }

            AsyncUtils.waitAll(futures);

            if (exception != null) {
                throw exception;
            }
        } else {
            // Execute action by plain loop
            for (Runnable task : tasks) {
                task.run();
            }
        }
    }
}
