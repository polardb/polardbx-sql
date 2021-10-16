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

package com.alibaba.polardbx.optimizer.planmanager;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.concurrent.TimeUnit;

/**
 * @author dylan
 */
public interface SystemTablePlanInfo {
    /**
     * check system table exists
     */
    Cache<String, Boolean> APPNAME_PLAN_INFO_ENABLED = CacheBuilder.newBuilder()
        .expireAfterWrite(1, TimeUnit.HOURS)
        .build();

    static void invalidateAll() {
        APPNAME_PLAN_INFO_ENABLED.invalidateAll();
    }

    void resetDataSource(DataSource dataSource);

    void createTableIfNotExist();

    boolean deleteAll(Connection conn);
}
