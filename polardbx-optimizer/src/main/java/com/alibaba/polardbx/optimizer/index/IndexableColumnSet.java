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

package com.alibaba.polardbx.optimizer.index;

import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticUtils;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import org.apache.calcite.rel.metadata.RelColumnOrigin;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author dylan
 */
public class IndexableColumnSet {

    // {schema -> table -> column}
    Map<String, Map<String, Set<String>>> m;

    public IndexableColumnSet() {
        this.m = new HashMap<>();
    }

    public void addIndexableColumn(RelColumnOrigin columnOrigin) {
        if (columnOrigin != null) {
            TableMeta tableMeta = CBOUtil.getTableMeta(columnOrigin.getOriginTable());
            String schemaName = tableMeta.getSchemaName();
            String tableName = tableMeta.getTableName();
            String columnName = columnOrigin.getColumnName();
            ColumnMeta columnMeta = tableMeta.getAllColumns().get(columnOrigin.getOriginColumnOrdinal());
            if (StatisticUtils.isBinaryOrJsonColumn(columnMeta)) {
                return;
            }
            if (DataTypeUtil.isStringType(columnMeta.getDataType())) {
                if (columnMeta.getField().getPrecision() > 1000) {
                    return;
                }
            }
            this.addIndexableColumn(schemaName, tableName, columnName);
        }
    }

    private void addIndexableColumn(String schemaName, String tableName, String columnName) {
        Map<String, Set<String>> t = m.get(schemaName);
        if (t == null) {
            t = new HashMap<>();
            m.put(schemaName, t);
        }
        Set<String> c = t.get(tableName);
        if (c == null) {
            c = new HashSet<>();
            t.put(tableName, c);
        }
        c.add(columnName);
    }
}
