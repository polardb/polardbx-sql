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

package com.alibaba.polardbx.executor.cursor.impl;

import com.alibaba.polardbx.executor.cursor.AbstractCursor;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.statis.OperatorStatisticsExt;
import com.alibaba.polardbx.optimizer.utils.QueryConcurrencyPolicy;
import org.apache.calcite.rel.RelNode;

import static com.alibaba.polardbx.executor.utils.ExecUtils.getQueryConcurrencyPolicy;

/**
 * @author chenghui.lch
 */
public class MyPhysicalCursor extends AbstractCursor {

    protected long cursorInstMemSize = 0;
    protected RelNode relNode = null;

    public MyPhysicalCursor() {
        super(false);
        initCursor();
    }

    protected void initCursor() {
        this.statistics = new OperatorStatisticsExt();
    }

    public RelNode getRelNode() {
        return relNode;
    }

    public void setRelNode(RelNode relNode) {
        this.relNode = relNode;
    }

    /**
     * For multi get, delay the initialization to next method. If
     * GROUP_CONCURRENT_BLOCK is set, initialization should call at first.
     */
    protected boolean isDelayInit(ExecutionContext executionContext) {
        QueryConcurrencyPolicy queryConcurrencyPolicy = getQueryConcurrencyPolicy(executionContext);

        switch (queryConcurrencyPolicy) {
        case GROUP_CONCURRENT_BLOCK:
        case RELAXED_GROUP_CONCURRENT:
        case CONCURRENT:
        case FIRST_THEN_CONCURRENT:
            return false;
        default:
            return true;
        }
    }
}
