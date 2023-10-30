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

package com.alibaba.polardbx.executor.utils;

import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlHelper;
import com.alibaba.polardbx.optimizer.config.schema.DefaultDbSchema;

import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeMap;

/**
 * Created by zhuqiwei.
 */

/**
 * This class is used to get the all charset and their bytes
 */
public class DrdsToAutoCharsetUtil {
    private final static Map<String, Integer> charsetAndLength = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    private static Integer maxCharsetLengthInBytes = null;

    public static synchronized Map<String, Integer> getAllCharsetAndLength() {
        if (charsetAndLength.isEmpty()) {
            final String queryCharsetSql = "show character set";
            List<Map<String, Object>> charsetInfoList = DdlHelper.getServerConfigManager().executeQuerySql(
                queryCharsetSql,
                DefaultDbSchema.NAME,
                null
            );

            for (Map<String, Object> info : charsetInfoList) {
                String charsetName = (String) info.get("Charset");
                Long len = (Long) info.get("Maxlen");
                charsetAndLength.put(charsetName.toLowerCase(), len.intValue());
            }
        }
        return charsetAndLength;
    }

    public static synchronized Integer getMaxCharsetLengthInBytes() {
        if (maxCharsetLengthInBytes == null) {
            Map<String, Integer> charsetInfo = getAllCharsetAndLength();
            try {
                maxCharsetLengthInBytes = charsetInfo.values().stream().max(Integer::compare).get();
            } catch (NoSuchElementException e) {
                maxCharsetLengthInBytes = 4;
            }
        }
        return maxCharsetLengthInBytes;
    }
}
