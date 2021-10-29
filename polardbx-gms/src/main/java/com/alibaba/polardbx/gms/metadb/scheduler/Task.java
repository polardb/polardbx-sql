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

package com.alibaba.polardbx.gms.metadb.scheduler;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * @author hongxi.chx
 */
public class Task implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(Task.class);
    private Cleanable cleanable;

    public Task(Cleanable cleanable) {
        this.cleanable = cleanable;
    }

    @Override
    public void run() {
        String cleanSql = cleanable.getCleanSql();
        try (Connection connection = MetaDbUtil.getConnection()) {
            Map<Integer, ParameterContext> params = new HashMap<>();
            Date date = new Date();
            Calendar cal = Calendar.getInstance();
            cal.setTime(date);
            cal.add(Calendar.DATE, 0 - cleanable.getKeepDays());
            MetaDbUtil.setParameter(1, params, ParameterMethod.setDate1, new java.sql.Date(cal.getTimeInMillis()));
            MetaDbUtil.delete(cleanSql, params, connection);
        } catch (Throwable e) {
            logger.error("Failed to clean table '" + cleanable.getCleanSql() + "'", e);
            //don't throw it
        }
    }

    public int getDelayHours() {
        return cleanable.getDelayDays() * 24;
    }

}


