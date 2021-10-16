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

package com.alibaba.polardbx.common.async;

import com.alibaba.polardbx.common.DefaultSchema;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.logger.MDC;

import java.util.Map;
import java.util.concurrent.Callable;

public class AsyncCallableTask<V> implements Callable<V> {

    protected static final Logger logger = LoggerFactory.getLogger(AsyncCallableTask.class);

    protected final Callable<V> task;
    protected final Map mdcContext;
    private final String schema;

    protected AsyncCallableTask(Callable<V> task, Map mdcContext, String schema) {
        this.task = task;
        this.mdcContext = mdcContext;
        this.schema = schema;
    }

    public static <V> AsyncCallableTask build(Callable<V> task) {
        final Map mdcContext = MDC.getCopyOfContextMap();
        final String schema = DefaultSchema.getSchemaName();
        return new AsyncCallableTask(task, mdcContext, schema);
    }

    @Override
    public V call() throws Exception {
        final Map savedMdcContext = MDC.getCopyOfContextMap();

        MDC.setContextMap(mdcContext);
        DefaultSchema.setSchemaName(schema);
        try {
            return task.call();
        } catch (Exception ex) {
            logger.error("Async callable task failed", ex);
            throw ex;
        } finally {
            MDC.setContextMap(savedMdcContext);
        }
    }
}
