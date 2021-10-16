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

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.Wrapper;
import org.apache.calcite.util.mapping.Mapping;

import java.util.List;
import java.util.function.Function;

/**
 * Writers for distinct input rows
 *
 * @author chenmo.cm
 */
public interface DistinctWriter extends SingleWriter, Wrapper {

    /**
     * @return mapping for columns used to get distinct input rows
     */
    Mapping getGroupingMapping();

    /**
     * Grouping input rows with columns specified by
     * {@link DistinctWriter#getGroupingMapping()}, get distinct rows and build
     * {@link PhyTableOperation}
     * for execute
     *
     * @param ec ExecutionContext
     * @param rowGenerator Deduplicate input rows
     * @return list of PhyTableOperation
     */
    List<RelNode> getInput(ExecutionContext ec, Function<DistinctWriter, List<List<Object>>> rowGenerator);
}
