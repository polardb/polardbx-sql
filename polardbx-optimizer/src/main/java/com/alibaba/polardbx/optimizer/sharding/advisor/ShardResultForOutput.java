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

package com.alibaba.polardbx.optimizer.sharding.advisor;

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.optimizer.parse.bean.SqlParameterized;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * This class stores the final sharding advice.
 * @author shengyu
 */
public class ShardResultForOutput {
    private final ShardColumnEdges shardColumnEdges;

    /**
     * shard advice for each component
     */
    private final List<ShardResult> results;

    private List<Pair<String, String>> unSharded;

    private Set<String> schemas;

    List<Pair<SqlParameterized, Integer>> sqls;
    public ShardResultForOutput(ShardColumnEdges shardColumnEdges, List<ShardResult> results) {
        this.shardColumnEdges = shardColumnEdges;
        this.results = results;
        buildUnSharded();
    }

    public Set<String> getSchemas() {
        return schemas;
    }

    public List<Pair<SqlParameterized, Integer>> getSqls() {
        return sqls;
    }

    public void setSqls(List<Pair<SqlParameterized, Integer>> sqls) {
        this.sqls = sqls;
    }

    /**
     * find tables not sharded by join graph
     */
    private void buildUnSharded() {
        List<String> sharded = new ArrayList<>();
        for (ShardResult result : results) {
            sharded.addAll(result.getIdToName());
        }
        this.schemas = new HashSet<>();
        unSharded = shardColumnEdges.getUnSharded(sharded, schemas);
    }

    /**
     * display the sharding advise
     *
     * @return an advice
     */
    public Map<String, StringBuilder> display() {
        // tables sharded by join graph
        Map<String, StringBuilder> resultSql = new TreeMap<>(String::compareToIgnoreCase);
        for (ShardResult result : results) {
            for(Map.Entry<String, StringBuilder> entry : result.display(shardColumnEdges).entrySet()) {
                if (resultSql.containsKey(entry.getKey())) {
                    resultSql.get(entry.getKey()).append(entry.getValue());
                } else {
                    resultSql.put(entry.getKey(), entry.getValue());
                }
            }
        }
        // tables not sharded by join graph
        for (Pair<String, String> entry : unSharded) {
            String[] split = entry.getKey().split("\\.");
            String schemaName = split[0];
            StringBuilder sb = new StringBuilder();
            if (entry.getValue() == null) {
                sb.append(AdvisorUtil.adviseSql(entry.getKey(), null, null));
            } else {
                sb.append(AdvisorUtil.adviseSql(entry.getKey(), entry.getValue(),
                    AdvisorUtil.getPartitions(entry.getKey())));
            }
            if (resultSql.containsKey(schemaName)) {
                resultSql.get(schemaName).append(sb);
            } else {
                resultSql.put(schemaName, sb);
            }
        }
        return resultSql;
    }

    public Map<String, Map<String, String>> getPartition() {
        // get the shard plan {schema -> table -> column}, 'column = null' means broadcast
        Map<String, Map<String, String>> partitions = new HashMap<>();
        for (String schemaName : schemas) {
            partitions.put(schemaName, new HashMap<>());
        }

        for (ShardResult result : results) {
            result.recordPartition(shardColumnEdges, partitions);
        }
        // tables not sharded by join graph
        for (Pair<String, String> entry : unSharded) {
            String [] st = entry.getKey().split("\\.");
            partitions.get(st[0]).put(st[1], entry.getValue());
        }
        return partitions;
    }
}
