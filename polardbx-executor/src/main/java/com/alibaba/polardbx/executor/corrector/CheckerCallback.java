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

package com.alibaba.polardbx.executor.corrector;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.apache.calcite.util.Pair;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public interface CheckerCallback {

    AtomicLong getPrimaryCounter();

    AtomicLong getGsiCounter();

    // Return false to close base select txn and restart this batch.

    void start(ExecutionContext baseEc, Checker checker);

    boolean batch(String logTbName,
                  String dbIndex,
                  String phyTable,
                  ExecutionContext selectEc,
                  Checker checker,
                  boolean primaryToGsi,
                  List<List<Pair<ParameterContext, byte[]>>> baseRows,
                  List<List<Pair<ParameterContext, byte[]>>> checkRows);

    void finish(ExecutionContext baseEc, Checker checker);

}
