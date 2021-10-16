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

package com.alibaba.polardbx.optimizer.core.rel.dml;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dml.util.ClassifyResult;
import com.alibaba.polardbx.optimizer.core.rel.dml.util.SourceRows;
import org.apache.calcite.util.Pair;

import java.util.List;
import java.util.Map;
import java.util.function.BiPredicate;

/**
 * @author chenmo.cm
 */
public interface CaseWhenWriter {

    /**
     * Classify source rows of dml as needed
     *
     * @param rowComparator For comparison between rows
     * @param sourceRows Input rows
     */
    ClassifyResult classify(BiPredicate<Writer, Pair<List<Object>, Map<Integer, ParameterContext>>> rowComparator,
                            SourceRows sourceRows, ExecutionContext ec, ClassifyResult result);
}
