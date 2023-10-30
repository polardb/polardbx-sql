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

package com.alibaba.polardbx.optimizer.core.rel;

import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.TableScan;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Created by zhuqiwei.
 *
 * @author zhuqiwei
 */
public class GatherReferencedGsiNameRelVisitor extends RelShuttleImpl {
    final protected Map<String, Set<String>> referencedTablesNames = new TreeMap<>(String::compareToIgnoreCase);

    @Override
    public RelNode visit(TableScan scan) {
        if (scan instanceof LogicalView) {
            LogicalView logicalView = (LogicalView) scan;
            String schemaName = logicalView.getSchemaName();
            if (schemaName != null && !referencedTablesNames.containsKey(schemaName)) {
                referencedTablesNames.put(schemaName, new TreeSet<>(String::compareToIgnoreCase));
            }
            referencedTablesNames.get(schemaName).addAll(logicalView.getTableNames());
        }
        return scan;
    }

    public Map<String, Set<String>> getReferencedGsiNames() {
        Map<String, Set<String>> referencedGsiNames = new TreeMap<>(String::compareToIgnoreCase);

        for (Map.Entry<String, Set<String>> entry : referencedTablesNames.entrySet()) {
            String schema = entry.getKey();
            Set<String> tables = entry.getValue();
            Set<String> gsiNames = new TreeSet<>(String::compareToIgnoreCase);
            for (String table : tables) {
                if (isGsi(schema, table)) {
                    gsiNames.add(table);
                }
            }

            if (!gsiNames.isEmpty()) {
                referencedGsiNames.put(schema, gsiNames);
            }
        }

        return referencedGsiNames;
    }

    protected boolean isGsi(String schemaName, String tableName) {
        OptimizerContext optimizerContext = OptimizerContext.getContext(schemaName);
        if (optimizerContext == null) {
            return false;
        }
        TableMeta tableMeta = optimizerContext.getLatestSchemaManager().getTable(tableName);
        if (tableMeta == null) {
            return false;
        }

        return tableMeta.isGsi();
    }
}
