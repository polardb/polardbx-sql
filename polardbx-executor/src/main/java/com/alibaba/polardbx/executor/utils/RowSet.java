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

package com.alibaba.polardbx.executor.utils;

import com.google.common.base.Preconditions;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.core.rel.dml.DistinctWriter;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author chenmo.cm
 */
public class RowSet {
    private final List<List<Object>> rows;
    private final List<ColumnMeta> metas;
    private final Map<DistinctWriter, List<List<Object>>> distinctRowSetCache = new ConcurrentHashMap<>();

    public RowSet(List<List<Object>> rows, List<ColumnMeta> metas) {
        Preconditions.checkNotNull(rows);
        Preconditions.checkNotNull(metas);

        this.rows = rows;
        this.metas = metas;
    }

    public List<List<Object>> distinctRowSetWithoutNull(DistinctWriter writer) {
        return distinctRowSetCache.computeIfAbsent(writer,
            t -> groupByColumns(rows, metas, writer.getGroupingMapping(), true));
    }

    private List<List<Object>> cachedDistinctRowSet(DistinctWriter writer) {
        return distinctRowSetCache.get(writer);
    }

    /**
     * Group by (sharding key + primary key) for each table to be modified
     *
     * @param values selected result
     * @param columns column meta of selected result
     * @param deduplicateMapping mapping for group column
     * @return values after GROUP BY
     */
    public static List<List<Object>> groupByColumns(List<List<Object>> values, List<ColumnMeta> columns,
                                                    Mapping deduplicateMapping, boolean skipNull) {
        if (null == deduplicateMapping) {
            return values;
        }

        final Map<GroupKey, List<Object>> groupKeyMap = new LinkedHashMap<>();
        final List<ColumnMeta> groupColumns = Mappings.permute(columns, deduplicateMapping);

        for (List<Object> row : values) {
            final List<Object> groupValues = Mappings.permute(row, deduplicateMapping);
            if (skipNull && groupValues.stream().allMatch(Objects::isNull)) {
                continue;
            }
            groupKeyMap.put(new GroupKey(groupValues.toArray(), groupColumns), row);
        }

        return new ArrayList<>(groupKeyMap.values());
    }

    public List<ColumnMeta> getMetas() {
        return metas;
    }
}
