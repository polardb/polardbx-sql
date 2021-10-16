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

import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import org.apache.calcite.rel.metadata.RelColumnOrigin;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author dylan
 */
public class CoverableColumnSet {

    // {schema -> table -> column}
    private Map<String, Map<String, Set<String>>> m;

    public CoverableColumnSet() {
        this.m = new HashMap<>();
    }

    public void addCoverableColumn(Set<RelColumnOrigin> columnOrigins) {
        if (columnOrigins != null) {
            for (RelColumnOrigin columnOrigin : columnOrigins) {
                String schemaName = CBOUtil.getTableMeta(columnOrigin.getOriginTable()).getSchemaName();
                String tableName = CBOUtil.getTableMeta(columnOrigin.getOriginTable()).getTableName();
                String columnName = columnOrigin.getColumnName();
                this.addCoverableColumn(schemaName, tableName, columnName);
            }
        }
    }

    private void addCoverableColumn(String schemaName, String tableName, String columnName) {
        schemaName = schemaName.toLowerCase();
        tableName = tableName.toLowerCase();
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

    public Set<String> getCoverableColumns(String schemaName, String tableNmae) {
        return m.get(schemaName.toLowerCase()).get(tableNmae.toLowerCase());
    }

    public void print() {
        for (String schemaName : m.keySet()) {
            for (String tableName : m.get(schemaName).keySet()) {
                System.out.println(schemaName + "." + tableName + ":" +
                    String.join(",", m.get(schemaName).get(tableName)));
            }
        }
    }
}
