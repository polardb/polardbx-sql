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

package com.alibaba.polardbx.server.response;

import com.alibaba.polardbx.CobarServer;
import com.alibaba.polardbx.config.SchemaConfig;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.sync.ISyncAction;

/**
 * @author mengshi.sunmengshi 2015年5月12日 下午1:28:16
 * @since 5.1.0
 */
public class ClearSQLSlowSyncAction implements ISyncAction {

    private String db;

    public ClearSQLSlowSyncAction() {
    }

    public ClearSQLSlowSyncAction(String db) {
        this.db = db;
    }

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }

    @Override
    public ResultCursor sync() {
        SchemaConfig schema = CobarServer.getInstance().getConfig().getSchemas().get(db);
        schema.getDataSource().getRecorder().clear();
        schema.getDataSource().getPhysicalRecorder().clear();
        return null;
    }
}
