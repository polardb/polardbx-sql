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

import com.alibaba.polardbx.common.TddlNode;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.sync.ISyncAction;
import com.alibaba.polardbx.gms.module.Module;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

/**
 * @author ljl
 */
public class ShowModuleSyncAction implements ISyncAction {
    private String db;
    private boolean full;
    private final static long SEVEN_DAYS = 7 * 24 * 60 * 60 * 1000L;

    public ShowModuleSyncAction() {
    }

    public ShowModuleSyncAction(boolean full) {
        this.full = full;
    }

    public ShowModuleSyncAction(String db, boolean full) {
        this.db = db;
        this.full = full;
    }

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }

    public boolean isFull() {
        return full;
    }

    public void setFull(boolean full) {
        this.full = full;
    }

    @Override
    public ResultCursor sync() {
        ArrayResultCursor result = new ArrayResultCursor("module");
        result.addColumn("MODULE_NAME", DataTypes.StringType);
        result.addColumn("HOST", DataTypes.StringType);
        result.addColumn("STATS", DataTypes.StringType);
        result.addColumn("STATUS", DataTypes.StringType);
        result.addColumn("RESOURCES", DataTypes.StringType);
        result.addColumn("SCHEDULE_JOBS", DataTypes.StringType);
        result.addColumn("VIEWS", DataTypes.StringType);
        result.addColumn("WORKLOAD", DataTypes.StringType);

        result.initMeta();

        for (Module m : Module.values()) {
            if (m.getModuleInfo() != null) {
                result.addRow(new Object[] {
                    m.name(),
                    TddlNode.getHost(),
                    m.getModuleInfo().state(),
                    m.getModuleInfo().status(System.currentTimeMillis() - SEVEN_DAYS),
                    m.getModuleInfo().resources(),
                    m.getModuleInfo().scheduleJobs(),
                    m.getModuleInfo().views(),
                    m.getModuleInfo().workload()
                });
            }
        }
        return result;
    }

}
