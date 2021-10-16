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

package com.alibaba.polardbx.rule.utils;

import com.alibaba.polardbx.common.properties.ConnectionProperties;

/**
 * @author chenghui.lch 2018年1月12日 下午1:59:30
 * @since 5.0.0
 */
public class CalcParamsAttribute {

    public final static String SHARD_FOR_EXTRA_DB = "shardForExtraDb";
    public final static String COM_DB_TB = "COM_DB_TB";
    public final static String SHARD_PARAMS = "SHARD_PARAMS";
    public final static String SHARD_DATATYPE_MAP = "SHARD_DATATYPE_MAP";
    public final static String SHARD_CHOISER = "SHARD_CHOISER";
    public final static String CONN_TIME_ZONE = ConnectionProperties.CONN_TIME_ZONE;
    public final static String DB_SHARD_KEY_SET = "DB_SHARD_KEY_SET";
    public final static String TB_SHARD_KEY_SET = "TB_SHARD_KEY_SET";
    public final static String COND_COL_IDX_MAP = "COND_COL_IDX_MAP";

}
