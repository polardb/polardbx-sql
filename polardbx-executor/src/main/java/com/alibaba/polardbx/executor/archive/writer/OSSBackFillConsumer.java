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

package com.alibaba.polardbx.executor.archive.writer;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.backfill.BatchConsumer;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class OSSBackFillConsumer implements BatchConsumer {
    private static final Logger LOGGER = LoggerFactory
        .getLogger("oss");

    private final Map<Pair<String, String>, OSSBackFillWriterTask> tasks = new ConcurrentHashMap<>();

    public OSSBackFillConsumer(Map<Pair<String, String>, OSSBackFillWriterTask> tasks) {
        this.tasks.putAll(tasks);
    }

    @Override
    public void consume(String sourcePhySchema, String sourcePhyTable, Cursor cursor, ExecutionContext context, List<Map<Integer, ParameterContext>> mockResult) {
        OSSBackFillWriterTask task = tasks.get(Pair.of(sourcePhySchema, sourcePhyTable));

        task.consume(cursor, mockResult, context);

        try {
            // Commit and close extract statement
            context.getTransaction().commit();
        } catch (Exception e) {
            LOGGER.error("Close extract statement failed!", e);
            throw GeneralUtil.nestedException("Unable to commit extract statement.", e);
        }
    }
}
