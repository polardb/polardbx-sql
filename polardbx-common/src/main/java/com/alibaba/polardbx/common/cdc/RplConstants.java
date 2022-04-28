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

package com.alibaba.polardbx.common.cdc;

/**
 * @author shicai.xsc 2021/3/5 11:20
 * @desc
 * @since 5.0.0.0
 */
public class RplConstants {
    public final static String MASTER_HOST = "MASTER_HOST";

    public final static String MASTER_USER = "MASTER_USER";

    public final static String MASTER_PASSWORD = "MASTER_PASSWORD";

    public final static String MASTER_PORT = "MASTER_PORT";

    public final static String MASTER_SERVER_ID = "MASTER_SERVER_ID";

    public final static String MASTER_LOG_FILE = "MASTER_LOG_FILE";

    public final static String MASTER_LOG_POS = "MASTER_LOG_POS";

    public final static String IGNORE_SERVER_IDS = "IGNORE_SERVER_IDS";

    public final static String SOURCE_HOST_TYPE = "SOURCE_HOST_TYPE";

    public final static String CHANNEL = "CHANNEL";

    public final static String REPLICATE_DO_DB = "REPLICATE_DO_DB";

    public final static String REPLICATE_IGNORE_DB = "REPLICATE_IGNORE_DB";

    public final static String REPLICATE_DO_TABLE = "REPLICATE_DO_TABLE";

    public final static String REPLICATE_IGNORE_TABLE = "REPLICATE_IGNORE_TABLE";

    public final static String REPLICATE_WILD_DO_TABLE = "REPLICATE_WILD_DO_TABLE";

    public final static String REPLICATE_WILD_IGNORE_TABLE = "REPLICATE_WILD_IGNORE_TABLE";

    public final static String REPLICATE_REWRITE_DB = "REPLICATE_REWRITE_DB";

    public final static String RUNNING = "RUNNING";

    public final static String LAST_ERROR = "LAST_ERROR";

    public final static String SKIP_COUNTER = "SKIP_COUNTER";

    public final static String REPLICATE_IGNORE_SERVER_IDS = "REPLICATE_IGNORE_SERVER_IDS";

    public final static String IS_ALL = "IS_ALL";
}
