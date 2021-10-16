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

package com.alibaba.polardbx.optimizer.config.table;

import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.TreeMaps;
import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.optimizer.exception.TableNotFoundException;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;

import java.util.Collection;
import java.util.EnumSet;
import java.util.Map;

/**
 * Created by fangwu on 2017/9/21.
 */
public class SimpleSchemaManager implements SchemaManager {

    private Map<String, TableMeta> tableMetaMap = TreeMaps.caseInsensitiveMap();

    private TddlRuleManager tddlRuleManager;

    private String schemaName;

    public SimpleSchemaManager(String schemaName, TddlRuleManager tddlRuleManager) {
        this.schemaName = schemaName;
        this.tddlRuleManager = tddlRuleManager;
    }

    @Override
    public TableMeta getTable(String tableName) {
        TableMeta tableMeta = tableMetaMap.get(tableName);
        if (tableMeta == null) {
            throw new TableNotFoundException(ErrorCode.ERR_TABLE_NOT_EXIST, tableName);
        } else {
            return tableMeta;
        }
    }

    @Override
    public void putTable(String tableName, TableMeta tableMeta) {
        tableMetaMap.put(tableName, tableMeta);
    }

    public Collection<TableMeta> getAllTables() {
        return tableMetaMap.values();
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

    }

    @Override
    public boolean isInited() {
        return false;
    }

    @Override
    public GsiMetaManager.GsiMetaBean getGsi(String primaryOrIndexTableName, EnumSet<IndexStatus> statusSet) {
        return GsiMetaManager.GsiMetaBean.empty();
    }

    @Override
    public String getSchemaName() {
        return schemaName;
    }

    @Override
    public TddlRuleManager getTddlRuleManager() {
        return tddlRuleManager;
    }
}
