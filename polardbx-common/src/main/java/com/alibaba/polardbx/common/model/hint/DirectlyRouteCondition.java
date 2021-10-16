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

package com.alibaba.polardbx.common.model.hint;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.alibaba.polardbx.common.utils.TreeMaps;

public class DirectlyRouteCondition extends ExtraCmdRouteCondition implements RouteCondition {

    protected String dbId;
    protected Set<String> tables = new HashSet<String>(2);
    protected List<Object> columns;

    protected Map<String, Object> sqlOptionParams = TreeMaps.synchronizeMap();

    public DirectlyRouteCondition() {

    }

    public DirectlyRouteCondition(String dbId) {
        this.dbId = dbId;
    }

    public Set<String> getTables() {
        return tables;
    }

    public void setTables(Set<String> tables) {
        this.tables = tables;
    }

    public void addTable(String table) {
        tables.add(table);
    }

    public void addATable(String table) {
        tables.add(table);
    }

    public String getDbId() {
        return dbId;
    }

    public void setDBId(String dbId) {
        this.dbId = dbId;
    }

    public List<Object> getColumns() {
        return columns;
    }

    public void setColumns(List<Object> columns) {
        this.columns = columns;
    }


    public Map<String, List<Map<String, String>>> getShardTableMap() {
        List<Map<String, String>> tableList =
            new ArrayList<Map<String, String>>(1);
        for (String targetTable : tables) {
            Map<String, String> table =
                new HashMap<String, String>(tables.size());
            table.put(virtualTableName, targetTable);
            if (!table.isEmpty()) {
                tableList.add(table);
            }
        }

        Map<String, List<Map<String, String>>> shardTableMap =
            new HashMap<String, List<Map<String, String>>>(2);
        shardTableMap.put(dbId, tableList);
        return shardTableMap;
    }

    public Map<String, Object> getSqlOptionParams() {
        return sqlOptionParams;
    }

    public void setSqlOptionParams(Map<String, Object> sqlOptionParams) {
        this.sqlOptionParams = sqlOptionParams;
    }

    public Object getSqlOptionParams(String key) {
        return sqlOptionParams.get(key);
    }

    public void putSqlOptionParams(String key, Object val) {
        sqlOptionParams.put(key, val);
    }

}
