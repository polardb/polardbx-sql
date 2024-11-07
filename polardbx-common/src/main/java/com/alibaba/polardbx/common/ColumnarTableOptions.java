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

public class ColumnarTableOptions {
    public static final String DICTIONARY_COLUMNS = "DICTIONARY_COLUMNS";
    public static final String TYPE = "type";
    public static final String SNAPSHOT_RETENTION_DAYS = "snapshot_retention_days";
    public static final String DEFAULT_SNAPSHOT_RETENTION_DAYS = "7";
    public static final String AUTO_GEN_COLUMNAR_SNAPSHOT_INTERVAL = "auto_gen_columnar_snapshot_interval";
    public static final String DEFAULT_AUTO_GEN_COLUMNAR_SNAPSHOT_INTERVAL = "30";
}
