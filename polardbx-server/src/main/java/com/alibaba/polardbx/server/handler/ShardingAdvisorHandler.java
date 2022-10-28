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

package com.alibaba.polardbx.server.handler;

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.executor.ExecutorHelper;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.sync.FetchPlanCacheSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.executor.whatIf.ShardingWhatIf;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.sharding.advisor.ShardResultForOutput;
import com.alibaba.polardbx.optimizer.sharding.advisor.ShardingAdvisor;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.server.response.ShardingAdvice;
import com.alibaba.polardbx.statistics.RuntimeStatHelper;
import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * @author shengyu
 */
public class ShardingAdvisorHandler {
    public static void handle(ByteString stmt, ServerConnection c, boolean hasMore) {
        try {
            Preconditions.checkArgument(c.getSchema() != null);
            // fetch plan caches
            List<List<Map<String, Object>>> results = SyncManagerHelper.sync(
                new FetchPlanCacheSyncAction(c.getSchema(), false, true),
                c.getSchema());
            Map<String, ShardingAdvisor.AdvisorCache> caches = new TreeMap<>(String::compareToIgnoreCase);
            for (List<Map<String, Object>> nodeRows : results) {
                if (nodeRows == null) {
                    continue;
                }
                for (Map<String, Object> row : nodeRows) {
                    final String id = DataTypes.StringType.convertFrom(row.get("ID"));
                    final Long hitCount = DataTypes.LongType.convertFrom(row.get("HIT_COUNT"));
                    final String sql = DataTypes.StringType.convertFrom(row.get("SQL"));
                    String parameter = DataTypes.StringType.convertFrom(row.get("PARAMETER"));
                    if (parameter == null) {
                        continue;
                    }
                    if (caches.containsKey(id)) {
                        caches.get(id).addHitCount(hitCount);
                    } else {
                        caches.put(id,
                            new ShardingAdvisor.AdvisorCache(hitCount, sql, JSON.parseObject(parameter, List.class)));
                    }
                }
            }

            // build shard plan
            ShardingAdvisor shardingAdvisor = new ShardingAdvisor(stmt);
            ShardResultForOutput result = shardingAdvisor.adviseFromPlanCache(c.getSchema(), caches);
            // build what if
            ShardingWhatIf shardingWhatIf = new ShardingWhatIf();
            shardingWhatIf.whatIf(result, c.getSchema(), shardingAdvisor.getParamManager());
            // response
            ShardingAdvice.response(c, hasMore, result, shardingWhatIf);
        } catch (Throwable ex) {
            ex.printStackTrace();
            throw ex;
        }
    }
}
