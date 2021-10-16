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

package com.alibaba.polardbx.executor.operator.util.bloomfilter;

import com.alibaba.polardbx.optimizer.chunk.Chunk;

import java.util.List;

/**
 * @author bairui.lrj
 * @since
 */
public class BloomFilterExpressionFilter {
    private final BloomFilterExpression expression;
    private final List<BloomFilterConsumeFilter> filters;

    public BloomFilterExpressionFilter(BloomFilterExpression expression) {
        this.expression = expression;
        this.filters = expression.buildFilters();
    }

    public boolean filter(Chunk.ChunkRow row) {
        if (expression.isExistBloomFilter()) {
            return filters.stream()
                .anyMatch(c -> c.filter(row));
        }
        return false;
    }

    public boolean isExistBloomFilter() {
        return expression.isExistBloomFilter();
    }
}
