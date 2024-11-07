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

package com.alibaba.polardbx.optimizer.hint.operator;

import java.util.List;

import com.google.common.collect.ImmutableList;

/**
 * @author chenmo.cm
 */
public class HintArgKey {

    public static final String PARAM_INDEX_MAP_KEY = "param_index_map";

    public static final List<HintArgKey> AGG_HINT = ImmutableList.of(HintArgKey.of("agg", 0),
        HintArgKey.of("group_by", 1),
        HintArgKey.of("having", 2),
        HintArgKey.of(PARAM_INDEX_MAP_KEY, 3));
    public static final List<HintArgKey> PROJECT_HINT = ImmutableList.of(HintArgKey.of("project", 0),
        HintArgKey.of(PARAM_INDEX_MAP_KEY, 1));
    public static final List<HintArgKey> FILTER_HINT = ImmutableList.of(HintArgKey.of("filter", 0),
        HintArgKey.of(PARAM_INDEX_MAP_KEY, 1));
    public static final List<HintArgKey> LIMIT_HINT = ImmutableList.of(HintArgKey.of("limit", 0),
        HintArgKey.of(PARAM_INDEX_MAP_KEY, 1));
    public static final List<HintArgKey> SORT_HINT = ImmutableList.of(HintArgKey.of("sort", 0),
        HintArgKey.of(PARAM_INDEX_MAP_KEY, 1));
    public static final List<HintArgKey> FROM_HINT = ImmutableList.of(HintArgKey.of("from", 0),
        HintArgKey.of(PARAM_INDEX_MAP_KEY, 1));
    public static final List<HintArgKey> UNION_HINT = ImmutableList.of(HintArgKey.of("all", 0));
    public static final List<HintArgKey> PUSHDOWN_HINT = ImmutableList.of(HintArgKey.of("table", 0),
        HintArgKey.of("node", 1),
        HintArgKey.of("condition", 2),
        HintArgKey.of("real_table", 3),
        HintArgKey.of("cross_single_table", 4),
        HintArgKey.of("qb_name", 5),
        HintArgKey.of(PARAM_INDEX_MAP_KEY, 6));
    public static final List<HintArgKey> SOCKET_TIMEOUT_HINT = ImmutableList.of(HintArgKey.of("timeout", 0));
    public static final List<HintArgKey> DYNAMIC_BROADCAST_JOIN_HINT = ImmutableList.of();
    public static final List<HintArgKey> JOIN_HINT = ImmutableList.of();
    public static final List<HintArgKey> PLAN_HINT = ImmutableList.of();
    public static final List<HintArgKey> MASTER_SLAVE_HINT = ImmutableList.of(HintArgKey.of("delay_cutoff", 0));
    public static final List<HintArgKey> SQL_DELAY_CUTOFF_HINT = ImmutableList.of(HintArgKey.of("delay", 0));
    public static final List<HintArgKey> CMD_EXTRA_HINT = ImmutableList.of();
    public static final List<HintArgKey> NODE_HINT = ImmutableList.of();
    public static final List<HintArgKey> SCAN_HINT = ImmutableList.of(HintArgKey.of("table", 0),
        HintArgKey.of("node", 1),
        HintArgKey.of("condition", 2),
        HintArgKey.of("real_table", 3),
        HintArgKey.of(PARAM_INDEX_MAP_KEY, 4));
    public static final List<HintArgKey> RANDOM_NODE_HINT = ImmutableList.of(HintArgKey.of("lst", 0),
        HintArgKey.of("num", 1),
        HintArgKey.of(PARAM_INDEX_MAP_KEY, 4));
    public static final List<HintArgKey> CORONADB_JSON_HINT = ImmutableList.of(HintArgKey.of("json", 0));
    public static final List<HintArgKey> MPP_HINT = ImmutableList.of(HintArgKey.of("rule_set", 0));
    public static final List<HintArgKey> QUERY_BLOCK_NAME_HINT = ImmutableList.of(HintArgKey.of("name", 0));
    public static final List<HintArgKey> MERGE_UNION_SIZE_HINT = ImmutableList.of(HintArgKey.of("merge_union_size", 0));
    public static final List<HintArgKey> INDEX_HINT = ImmutableList.of(HintArgKey.of("table", 0),
        HintArgKey.of("index", 1), HintArgKey.of("local_index", 2));
    public static final List<HintArgKey> INVENTORY_HINT = ImmutableList.of(HintArgKey.of("commit_on_success", 0),
        HintArgKey.of("rollback_on_fail", 1), HintArgKey.of("target_affect_row", 2), HintArgKey.of("row number", 3));

    public final Integer ordinal;
    private final String name;

    public HintArgKey(String name, Integer ordinal) {
        this.name = name;
        this.ordinal = ordinal;
    }

    public static HintArgKey of(String name, Integer ordinal) {
        return new HintArgKey(name, ordinal);
    }

    public String getName() {
        return name.toUpperCase();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof HintArgKey)) {
            return false;
        }

        HintArgKey that = (HintArgKey) o;

        return ordinal.equals(that.ordinal);
    }

    @Override
    public int hashCode() {
        return ordinal.hashCode();
    }
}
