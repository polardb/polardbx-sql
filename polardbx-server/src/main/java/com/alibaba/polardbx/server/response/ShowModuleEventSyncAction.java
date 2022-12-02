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
import com.alibaba.polardbx.gms.module.Module;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.sync.ISyncAction;
import com.alibaba.polardbx.gms.module.LogUnit;
import com.alibaba.polardbx.gms.module.ModuleLogInfo;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

import java.util.Date;
import java.util.Iterator;

/**
 * @author ljl
 */
public class ShowModuleEventSyncAction implements ISyncAction {
    private String db;
    private boolean full;

    public ShowModuleEventSyncAction() {
    }

    public ShowModuleEventSyncAction(boolean full) {
        this.full = full;
    }

    public ShowModuleEventSyncAction(String db, boolean full) {
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

        ArrayResultCursor result = new ArrayResultCursor("processlist");
        result.addColumn("MODULE_NAME", DataTypes.StringType);
        result.addColumn("HOST", DataTypes.StringType);
        result.addColumn("TIMESTAMP", DataTypes.TimestampType);
        result.addColumn("LOG_PATTERN", DataTypes.StringType);
        result.addColumn("LEVEL", DataTypes.StringType);
        result.addColumn("EVENT", DataTypes.StringType);
        result.addColumn("TRACE_INFO", DataTypes.StringType);

        result.initMeta();

        for (Module m : Module.values()) {
            Iterator<LogUnit> it = ModuleLogInfo.getInstance().logRequireLogUnit(m, -1);
            while (it.hasNext()) {
                LogUnit u = it.next();
                result.addRow(new Object[] {
                    m.name(),
                    TddlNode.getHost(),
                    new Date(u.getTimestamp()),
                    u.getLp(),
                    u.getLevel(),
                    u.logWithoutTimestamp(),
                    u.getTraceInfo()
                });
            }
        }

        return result;
    }

}
