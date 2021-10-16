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

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

/**
 * @author chenmo.cm
 */
public enum HintType {
    /**
     * none
     */
    NONE(Values.NONE),
    /**
     * pushdown
     */
    PUSHDOWN(Values.PUSHDOWN),
    /**
     * construct
     */
    CONSTRUCT(Values.CONSTRUCT),
    /**
     * push
     */
    PUSH_SORT(Values.PUSH_SORT),
    PUSH_PJ(Values.PUSH_PJ),
    PUSH_AGG(Values.PUSH_AGG),
    PUSH_FT(Values.PUSH_FT),
    PUSH_FROM(Values.PUSH_FROM),
    PUSH_LMT(Values.PUSH_LMT),
    /**
     * add
     */
    ADD_MS(Values.ADD_MS),
    ADD_TS(Values.ADD_TS),
    ADD_PJ(Values.ADD_PJ),
    ADD_AGG(Values.ADD_AGG),
    ADD_FT(Values.ADD_FT),
    ADD_LMT(Values.ADD_LMT),
    ADD_UN(Values.ADD_UN),
    /**
     * cmd
     */
    CMD_MASTER(Values.CMD_MASTER),
    CMD_SLAVE(Values.CMD_SLAVE),
    CMD_SOCKET_TIMEOUT(Values.CMD_SOCKET_TIMEOUT),
    CMD_DYNAMIC_BROADCAST_JOIN(Values.CMD_DYNAMIC_BROADCAST_JOIN),
    CMD_SQL_DELAY_CUTOFF(Values.CMD_SQL_DELAY_CUTOFF),
    CMD_EXTRA(Values.CMD_EXTRA),
    CMD_NODE(Values.CMD_NODE),
    CMD_SCAN(Values.CMD_SCAN),
    CMD_CORONADB_JSON(Values.CMD_CORONADB_JSON),
    CMD_MPP(Values.CMD_MPP),
    CMD_MERGE_UNION_SIZE(Values.CMD_MERGE_UNION_SIZE),
    CMD_QUERY_BLOCK_NAME(Values.CMD_QUERY_BLOCK_NAME),
    CMD_INDEX(Values.INDEX),
    CMD_BKA_JOIN(Values.BKA_JOIN),
    CMD_NL_JOIN(Values.NL_JOIN),
    CMD_HASH_JOIN(Values.HASH_JOIN),
    CMD_HASH_OUTER_JOIN(Values.HASH_OUTER_JOIN),
    CMD_MATERIALIZED_SEMI_JOIN(Values.MATERIALIZED_SEMI_JOIN),
    CMD_SEMI_HASH_JOIN(Values.SEMI_HASH_JOIN),
    CMD_SEMI_SORT_MERGE_JOIN(Values.SEMI_SORT_MERGE_JOIN),
    CMD_SEMI_NL_JOIN(Values.SEMI_NL_JOIN),
    CMD_SEMI_BKA_JOIN(Values.SEMI_BKA_JOIN),
    CMD_ANTI_NL_JOIN(Values.ANTI_NL_JOIN),
    CMD_SORT_MERGE_JOIN(Values.SORT_MERGE_JOIN),
    CMD_NO_JOIN(Values.NO_JOIN),
    CMD_ANTI_HASH_JOIN(Values.ANTI_HASH_JOIN),
    CMD_HASH_GROUP_JOIN(Values.HASH_GROUP_JOIN),

    /**
     * SPM PLAN
     */
    CMD_PLAN(Values.PLAN),

    /**
     * inventory hint
     */
    INVENTORY_COMMIT_ON_SUCCESS(Values.COMMIT_ON_SUCCESS),
    INVENTORY_ROLLBACK_ON_FAIL(Values.ROLLBACK_ON_FAIL),
    INVENTORY_TARGET_AFFECT_ROW(Values.TARGET_AFFECT_ROW);

    public static final EnumSet<HintType> PUSH_HINT = EnumSet.of(PUSH_AGG,
        PUSH_FROM,
        PUSH_FT,
        PUSH_LMT,
        PUSH_PJ,
        PUSH_SORT);
    public static final EnumSet<HintType> ADD_HINT = EnumSet.of(ADD_AGG,
        ADD_FT,
        ADD_LMT,
        ADD_PJ,
        ADD_MS,
        ADD_TS,
        ADD_UN);
    public static final EnumSet<HintType> CMD_HINT = EnumSet.of(CMD_MASTER,
        CMD_SLAVE,
        CMD_SOCKET_TIMEOUT,
        CMD_SQL_DELAY_CUTOFF,
        CMD_EXTRA,
        CMD_NODE,
        CMD_NODE,
        CMD_CORONADB_JSON,
        CMD_MERGE_UNION_SIZE,
        CMD_QUERY_BLOCK_NAME,
        CMD_BKA_JOIN,
        CMD_NL_JOIN,
        CMD_HASH_JOIN,
        CMD_SORT_MERGE_JOIN,
        CMD_MATERIALIZED_SEMI_JOIN,
        CMD_SEMI_SORT_MERGE_JOIN,
        CMD_SEMI_HASH_JOIN,
        CMD_SEMI_NL_JOIN,
        CMD_NO_JOIN,
        CMD_HASH_GROUP_JOIN,
        CMD_INDEX);
    public static final EnumSet<HintType> PUSHDOWN_HINT = EnumSet.of(PUSHDOWN);
    public static final EnumSet<HintType> ILLEGAL_HINT = EnumSet.of(NONE);
    private static final Map<String, HintType> VALUE_TO_OPERATOR = new HashMap<>();

    static {
        VALUE_TO_OPERATOR.put(Values.PUSH_SORT, PUSH_SORT);
        VALUE_TO_OPERATOR.put(Values.PUSH_PJ, PUSH_PJ);
        VALUE_TO_OPERATOR.put(Values.PUSH_AGG, PUSH_AGG);
        VALUE_TO_OPERATOR.put(Values.PUSH_FT, PUSH_FT);
        VALUE_TO_OPERATOR.put(Values.PUSH_FROM, PUSH_FROM);
        VALUE_TO_OPERATOR.put(Values.PUSH_LMT, PUSH_LMT);
        VALUE_TO_OPERATOR.put(Values.ADD_MS, ADD_MS);
        VALUE_TO_OPERATOR.put(Values.ADD_TS, ADD_TS);
        VALUE_TO_OPERATOR.put(Values.ADD_PJ, ADD_PJ);
        VALUE_TO_OPERATOR.put(Values.ADD_AGG, ADD_AGG);
        VALUE_TO_OPERATOR.put(Values.ADD_FT, ADD_FT);
        VALUE_TO_OPERATOR.put(Values.ADD_LMT, ADD_LMT);
        VALUE_TO_OPERATOR.put(Values.ADD_UN, ADD_UN);
        VALUE_TO_OPERATOR.put(Values.PUSHDOWN, PUSHDOWN);
        VALUE_TO_OPERATOR.put(Values.CONSTRUCT, CONSTRUCT);
        VALUE_TO_OPERATOR.put(Values.CMD_MASTER, CMD_MASTER);
        VALUE_TO_OPERATOR.put(Values.CMD_SLAVE, CMD_SLAVE);
        VALUE_TO_OPERATOR.put(Values.CMD_SOCKET_TIMEOUT, CMD_SOCKET_TIMEOUT);
        VALUE_TO_OPERATOR.put(Values.CMD_DYNAMIC_BROADCAST_JOIN, CMD_DYNAMIC_BROADCAST_JOIN);
        VALUE_TO_OPERATOR.put(Values.CMD_SQL_DELAY_CUTOFF, CMD_SQL_DELAY_CUTOFF);
        VALUE_TO_OPERATOR.put(Values.CMD_EXTRA, CMD_EXTRA);
        VALUE_TO_OPERATOR.put(Values.CMD_NODE, CMD_NODE);
        VALUE_TO_OPERATOR.put(Values.CMD_SCAN, CMD_SCAN);
        VALUE_TO_OPERATOR.put(Values.CMD_CORONADB_JSON, CMD_CORONADB_JSON);
        VALUE_TO_OPERATOR.put(Values.CMD_MERGE_UNION_SIZE, CMD_MERGE_UNION_SIZE);
        VALUE_TO_OPERATOR.put(Values.CMD_MPP, CMD_MPP);
        VALUE_TO_OPERATOR.put(Values.CMD_QUERY_BLOCK_NAME, CMD_QUERY_BLOCK_NAME);
        VALUE_TO_OPERATOR.put(Values.INDEX, CMD_INDEX);
        VALUE_TO_OPERATOR.put(Values.BKA_JOIN, CMD_BKA_JOIN);
        VALUE_TO_OPERATOR.put(Values.HASH_JOIN, CMD_HASH_JOIN);
        VALUE_TO_OPERATOR.put(Values.HASH_OUTER_JOIN, CMD_HASH_OUTER_JOIN);
        VALUE_TO_OPERATOR.put(Values.NL_JOIN, CMD_NL_JOIN);
        VALUE_TO_OPERATOR.put(Values.SORT_MERGE_JOIN, CMD_SORT_MERGE_JOIN);
        VALUE_TO_OPERATOR.put(Values.MATERIALIZED_SEMI_JOIN, CMD_MATERIALIZED_SEMI_JOIN);
        VALUE_TO_OPERATOR.put(Values.SEMI_HASH_JOIN, CMD_SEMI_HASH_JOIN);
        VALUE_TO_OPERATOR.put(Values.SEMI_NL_JOIN, CMD_SEMI_NL_JOIN);
        VALUE_TO_OPERATOR.put(Values.SEMI_BKA_JOIN, CMD_SEMI_BKA_JOIN);
        VALUE_TO_OPERATOR.put(Values.ANTI_NL_JOIN, CMD_ANTI_NL_JOIN);
        VALUE_TO_OPERATOR.put(Values.NO_JOIN, CMD_NO_JOIN);
        VALUE_TO_OPERATOR.put(Values.ANTI_HASH_JOIN, CMD_ANTI_HASH_JOIN);
        VALUE_TO_OPERATOR.put(Values.SEMI_SORT_MERGE_JOIN, CMD_SEMI_SORT_MERGE_JOIN);
        VALUE_TO_OPERATOR.put(Values.PLAN, CMD_PLAN);
        VALUE_TO_OPERATOR.put(Values.COMMIT_ON_SUCCESS, INVENTORY_COMMIT_ON_SUCCESS);
        VALUE_TO_OPERATOR.put(Values.ROLLBACK_ON_FAIL, INVENTORY_ROLLBACK_ON_FAIL);
        VALUE_TO_OPERATOR.put(Values.TARGET_AFFECT_ROW, INVENTORY_TARGET_AFFECT_ROW);
        VALUE_TO_OPERATOR.put(Values.HASH_GROUP_JOIN, CMD_HASH_GROUP_JOIN);
    }

    private final String value;

    HintType(String value) {
        this.value = value;
    }

    public static HintType of(String hint) {
        if (null == hint || !VALUE_TO_OPERATOR.containsKey(hint.toUpperCase())) {
            return NONE;
        }

        return VALUE_TO_OPERATOR.get(hint.toUpperCase());
    }

    public boolean isA(EnumSet set) {
        return set.contains(this);
    }

    public String getValue() {
        return value;
    }

    private static class Values {

        static final String NONE = "NONE";
        static final String PUSH_SORT = "PUSH_SORT";
        static final String PUSH_PJ = "PUSH_PJ";
        static final String PUSH_AGG = "PUSH_AGG";
        static final String PUSH_FT = "PUSH_FT";
        static final String PUSH_FROM = "PUSH_FROM";
        static final String PUSH_LMT = "PUSH_LMT";
        static final String ADD_MS = "ADD_MS";
        static final String ADD_TS = "ADD_TS";
        static final String ADD_PJ = "ADD_PJ";
        static final String ADD_AGG = "ADD_AGG";
        static final String ADD_FT = "ADD_FT";
        static final String ADD_LMT = "ADD_LMT";
        static final String ADD_UN = "ADD_UN";
        static final String PUSHDOWN = "PUSHDOWN";
        static final String CONSTRUCT = "CONSTRUCT";
        static final String CMD_MASTER = "MASTER";
        static final String CMD_SLAVE = "SLAVE";
        static final String CMD_SOCKET_TIMEOUT = "SOCKET_TIMEOUT";
        static final String CMD_DYNAMIC_BROADCAST_JOIN = "DBJ";
        static final String CMD_SQL_DELAY_CUTOFF = "SQL_DELAY_CUTOFF";
        static final String CMD_EXTRA = "CMD_EXTRA";
        static final String CMD_NODE = "NODE";
        static final String CMD_SCAN = "SCAN";
        static final String CMD_CORONADB_JSON = "__CDB_JSON_HINT__";
        static final String CMD_MERGE_UNION_SIZE = "MERGE_UNION_SIZE";
        static final String CMD_MPP = "MPP";
        static final String CMD_QUERY_BLOCK_NAME = "QB_NAME";
        static final String INDEX = "INDEX";
        static final String BKA_JOIN = "BKA_JOIN";
        static final String NL_JOIN = "NL_JOIN";
        static final String HASH_JOIN = "HASH_JOIN";
        static final String HASH_OUTER_JOIN = "HASH_OUTER_JOIN";
        static final String SORT_MERGE_JOIN = "SORT_MERGE_JOIN";
        static final String MATERIALIZED_SEMI_JOIN = "MATERIALIZED_SEMI_JOIN";
        static final String SEMI_NL_JOIN = "SEMI_NL_JOIN";
        static final String ANTI_NL_JOIN = "ANTI_NL_JOIN";
        static final String SEMI_HASH_JOIN = "SEMI_HASH_JOIN";
        static final String SEMI_BKA_JOIN = "SEMI_BKA_JOIN";
        static final String SEMI_SORT_MERGE_JOIN = "SEMI_SORT_MERGE_JOIN";
        static final String NO_JOIN = "NO_JOIN";
        static final String ANTI_HASH_JOIN = "ANTI_HASH_JOIN";
        static final String HASH_GROUP_JOIN = "HASH_GROUP_JOIN";
        static final String PLAN = "PLAN";
        static final String COMMIT_ON_SUCCESS = "COMMIT_ON_SUCCESS";
        static final String ROLLBACK_ON_FAIL = "ROLLBACK_ON_FAIL";
        static final String TARGET_AFFECT_ROW = "TARGET_AFFECT_ROW";
    }
}
