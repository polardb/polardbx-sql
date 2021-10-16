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

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.google.common.base.Preconditions;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.model.sqljep.Comparative;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.timezone.InternalTimeZone;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.optimizer.biv.MockDataManager;
import com.alibaba.polardbx.rule.TableRule;
import com.alibaba.polardbx.rule.model.TargetDB;
import com.alibaba.polardbx.rule.utils.CalcParamsAttribute;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author lingce.ldm 2017-12-08 14:40
 */
public class NormalShardProcessor extends ShardProcessor {

    private String schemaName;
    private String tableName;
    private List<String> columns;
    Map<String, Comparative> comparatives;
    private Map<String, DataType> dataType;
    private Map<String, Integer> condIndex;

    protected NormalShardProcessor(String schemaName,
                                   String tableName,
                                   List<String> columns,
                                   TableRule tableRule,
                                   Map<String, Comparative> comparatives,
                                   Map<String, DataType> dataTypeMap,
                                   Map<String, Integer> condColValIdxMap) {
        super(tableRule);
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.columns = columns;
        this.condIndex = condColValIdxMap;
        this.dataType = dataTypeMap;
        this.comparatives = comparatives;

    }

    @Override
    Pair<String, String> shard(Map<Integer, ParameterContext> param, ExecutionContext executionContext) {
        boolean isWrite = false;
        boolean forceAllowFullTableScan = true;
        Map<String, Comparative> comparatives = new HashMap<String, Comparative>();
        Map<String, Object> calcParams = new HashMap<>();

        InternalTimeZone timeZone = null;
        if (executionContext != null) {
            timeZone = executionContext.getTimeZone();
        }
        calcParams.put(CalcParamsAttribute.CONN_TIME_ZONE, timeZone);
        calcParams.put(CalcParamsAttribute.SHARD_DATATYPE_MAP, dataType);
        calcParams.put(CalcParamsAttribute.COND_COL_IDX_MAP, condIndex);

        List<TargetDB> targetDBs = executionContext.getSchemaManager(schemaName)
            .getTddlRuleManager()
            .shard(tableName, forceAllowFullTableScan, isWrite, comparatives, param, calcParams, executionContext);

        Preconditions.checkArgument(targetDBs.size() == 1);
        TargetDB tdb = targetDBs.get(0);
        // record phy table refer to logical table in mock mode
        if (ConfigDataMode.isFastMock()) {
            for (String phyTable : tdb.getTableNames()) {
                MockDataManager.phyTableToLogicalTableName.put(phyTable, tableName);
            }
        }

        Set<String> tableNames = tdb.getTableNames();
        Preconditions.checkArgument(tableNames.size() == 1);
        return new Pair<>(tdb.getDbIndex(), tableNames.iterator().next());
    }

}
