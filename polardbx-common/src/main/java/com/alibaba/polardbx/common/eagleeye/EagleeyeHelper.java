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

package com.alibaba.polardbx.common.eagleeye;

import com.alibaba.polardbx.common.constants.SystemTables;

public class EagleeyeHelper {

    public static final String TEST_TABLE_PREFIX = "__test_";

    public static String rebuildTableName(String tableName, boolean testMode) {
        if (!testMode) {
            return tableName;
        }
        if (tableName.startsWith(TEST_TABLE_PREFIX)) {
            return tableName;
        }
        String newTable = tableName.toUpperCase();
        if (SystemTables.contains(newTable)) {
            return tableName;
        }
        return EagleeyeHelper.TEST_TABLE_PREFIX + tableName;
    }
}
