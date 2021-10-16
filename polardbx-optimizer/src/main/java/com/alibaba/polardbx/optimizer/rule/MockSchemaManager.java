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

package com.alibaba.polardbx.optimizer.rule;

import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.gms.metadb.table.TableStatus;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.IndexMeta;
import com.alibaba.polardbx.optimizer.config.table.IndexType;
import com.alibaba.polardbx.optimizer.config.table.Relationship;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.exception.TableNotFoundException;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

public class MockSchemaManager extends AbstractLifecycle implements SchemaManager {

    private Map<String, TableMeta> tableMetaMap = new HashMap<>();

    private TddlRuleManager tddlRule;

    @Override
    public TableMeta getTable(String tableName) {
        if (tableName.equalsIgnoreCase(DUAL)) {
            return buildDualTable();
        }

        if (tableMetaMap.containsKey(tableName.toUpperCase())) {
            return tableMetaMap.get(tableName.toUpperCase());
        } else {
            String error = String.format("Table %s not exists, existing tables: %s, hashcode: %s",
                tableName.toUpperCase(),
                StringUtils.join(tableMetaMap.keySet().toArray(), " ,"),
                this.hashCode());
            throw new TableNotFoundException(ErrorCode.ERR_TABLE_NOT_EXIST, tableName);
        }
    }

    public TableMeta buildDualTable() {
        IndexMeta index = new IndexMeta(SchemaManager.DUAL,
            new ArrayList<ColumnMeta>(),
            new ArrayList<ColumnMeta>(),
            IndexType.NONE,
            Relationship.NONE,
            false,
            true,
            true,
            "");

        return new TableMeta(DUAL, new ArrayList<ColumnMeta>(), index, new ArrayList<IndexMeta>(), true,
            TableStatus.PUBLIC, 0);
    }

    @Override
    public void putTable(String tableName, TableMeta tableMeta) {
        tableMetaMap.put(tableName.toUpperCase(), tableMeta);
    }

    @Override
    public void reload(String tableName) {
    }

    @Override
    public void invalidate(String tableName) {
    }

    @Override
    public void invalidateAll() {
    }

    @Override
    public void init() {

    }

    @Override
    public void destroy() {
        tableMetaMap.clear();
    }

    @Override
    public boolean isInited() {
        return true;
    }

    @Override
    public GsiMetaManager.GsiMetaBean getGsi(String primaryOrIndexTableName, EnumSet<IndexStatus> statusSet) {
        return GsiMetaManager.GsiMetaBean.empty();
    }

    @Override
    public String getSchemaName() {
        return "mock";
    }

    @Override
    public TddlRuleManager getTddlRuleManager() {
        return tddlRule;
    }

    public void setTddlRule(TddlRuleManager tddlRule) {
        this.tddlRule = tddlRule;
    }
}
