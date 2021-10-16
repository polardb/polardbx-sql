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

package com.alibaba.polardbx.common;


public class TddlConstants {

    public static final long DEFAULT_TABLE_META_EXPIRE_TIME = 300 * 1000;

    public static final long DEFAULT_VIEW_CACHE_EXPIRE_TIME = 300 * 1000;

    public static final int DEFAULT_OPTIMIZER_EXPIRE_TIME = 12 * 3600 * 1000;
    public static final int DEFAULT_DIGEST_EXPIRE_TIME = 300 * 1000;

    public static final int MIN_OPTIMIZER_CACHE_SIZE = 1000;
    public static final int DEFAULT_OPTIMIZER_CACHE_SIZE = 2000;
    public static final int MAX_OPTIMIZER_CACHE_SIZE = 10000;
    public static final int DEFAULT_PARSER_CACHE_SIZE = 1000;
    public static final int DEFAULT_DIGEST_CACHE_SIZE = 1000;

    public static final int DEFAULT_CCL_CACHE_NOT_MATCH_CONN_SIZE = 20000;
    public static final int DEFAULT_CCL_CACHE_NOT_MATCH_PLAN_SIZE = 20000;
    public static final int DEFAULT_CCL_MATCH_RULE_SIZE = 20000;

    public static final int DEFAULT_STREAM_THRESOLD = 100;

    public static final int DEFAULT_CONCURRENT_THREAD_SIZE = 8;

    public static final int MAX_CONCURRENT_THREAD_SIZE = 256;

    public static final long MAX_CACHED_SQL_LENGTH = 2048L;

    public static final String DS_MODE_AP = "ap";

    public static final long DEFAULT_RETAIN_HOURS = 2L;

    public static final String RULE_BROADCAST = "rule.broadcast";
    public static final String RULE_ALLOW_FULL_TABLE_SCAN = "rule.allowFullTableScan";

    public static final long MAX_EXECUTE_MEMORY = 200L;
    public static final String EXPLAIN = "explain";
    public static final String INFORMATION_SCHEMA = "INFORMATION_SCHEMA";
    public static final String ANONAMOUS_DBKEY = "ANONAMOUS";
    public static final String CURRENT_DBKEY = "CURRENT_DBKEY";

    public static final long DML_SELECT_BATCH_SIZE_DEFAULT = 1000L;

    public static final long DML_SELECT_LIMIT_DEFAULT = 1000000L;

    public static final String IMPLICIT_COL_NAME = "_drds_implicit_id_";
    public static final String IMPLICIT_KEY_NAME = "_drds_implicit_pk_";

    public static final String UGSI_PK_INDEX_NAME = "_gsi_pk_idx_";

    public static final String AUTO_LOCAL_INDEX_PREFIX = "_local_";
}
