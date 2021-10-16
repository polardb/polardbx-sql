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

package com.alibaba.polardbx.optimizer.config.table.statistic;

import java.sql.Timestamp;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * @author dylan
 */
public class StatisticLogInfo {

    private static int MAX_LOG_SIZE = 2000;

    private List<String> logList;

    public StatisticLogInfo() {
        this.logList = Collections.synchronizedList(new LinkedList<>());
    }

    public void add(String log) {
        if (logList.size() > MAX_LOG_SIZE) {
            logList.remove(0);
        }
        StringBuilder stringBuilder = new StringBuilder();
        Timestamp timestamp = new Timestamp(System.currentTimeMillis());
        stringBuilder.append(timestamp);
        stringBuilder.append(" ");
        stringBuilder.append(log);
        logList.add(stringBuilder.toString());
    }

    public List<String> getLogList() {
        return logList;
    }
}
