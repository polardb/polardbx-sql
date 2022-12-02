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

package com.alibaba.polardbx.executor.backfill;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;

import java.util.List;
import java.util.Map;

public interface BatchConsumer {

    default void consume(List<Map<Integer, ParameterContext>> batch,
                         Pair<ExecutionContext, Pair<String, String>> extractEcAndIndexPair) {
        throw new UnsupportedOperationException();
    }

    default void consume(String sourcePhySchema, String sourcePhyTable, Cursor cursor, ExecutionContext context, List<Map<Integer, ParameterContext>> mockResult) {
        throw new UnsupportedOperationException();
    }
}
