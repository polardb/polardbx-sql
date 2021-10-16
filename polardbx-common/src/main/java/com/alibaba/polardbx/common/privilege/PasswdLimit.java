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

package com.alibaba.polardbx.common.privilege;

import java.util.Map;

public class PasswdLimit {
    private int maxErrorCount;
    private Map<String, Integer> currentErrorCountMap;

    public PasswdLimit(int maxErrorCount, Map<String, Integer> currentErrorCountMap) {
        this.maxErrorCount = maxErrorCount;
        this.currentErrorCountMap = currentErrorCountMap;
    }

    public boolean checkUserLoginErrorMaxCount(String userName) {
        if (maxErrorCount <= 0) {
            return true;
        }
        String key = userName.toUpperCase();
        if (currentErrorCountMap != null) {
            Integer currErrCount = currentErrorCountMap.get(key);
            if (currErrCount == null) {
                currErrCount = 0;
            }
            if (currErrCount >= maxErrorCount) {
                return false;
            }
        }
        return true;
    }

    public boolean increment(String userName) {
        String key = userName.toUpperCase();
        Integer integer = 0;
        if (currentErrorCountMap.containsKey(key)) {
            integer = currentErrorCountMap.get(key);
        }
        integer++;
        currentErrorCountMap.put(key, integer);

        return true;
    }
}
