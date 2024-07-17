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

package com.alibaba.polardbx.gms.metadb.misc;

import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.scheduler.FiredScheduledJobsAccessor;
import com.alibaba.polardbx.gms.scheduler.ScheduledJobsAccessor;

import java.sql.Connection;

public class SchemaInfoCleaner extends AbstractAccessor {

    private final DdlEngineAccessor ddlEngineAccessor;
    private final ReadWriteLockAccessor readWriteLockAccessor;
    private final DdlEngineTaskAccessor ddlEngineTaskAccessor;
    private final ScheduledJobsAccessor scheduledJobsAccessor;
    private final FiredScheduledJobsAccessor firedScheduledJobsAccessor;

    public SchemaInfoCleaner() {
        ddlEngineAccessor = new DdlEngineAccessor();
        ddlEngineTaskAccessor = new DdlEngineTaskAccessor();
        readWriteLockAccessor = new ReadWriteLockAccessor();
        scheduledJobsAccessor = new ScheduledJobsAccessor();
        firedScheduledJobsAccessor = new FiredScheduledJobsAccessor();
    }

    @Override
    public void setConnection(Connection connection) {
        super.setConnection(connection);
        ddlEngineAccessor.setConnection(connection);
        readWriteLockAccessor.setConnection(connection);
        ddlEngineTaskAccessor.setConnection(connection);
        scheduledJobsAccessor.setConnection(connection);
        firedScheduledJobsAccessor.setConnection(connection);
    }

    public void removeAll(String schemaName) {
        ddlEngineAccessor.deleteAll(schemaName);
        //ddlEngineAccessor.deleteAllArchive(schemaName);
        ddlEngineTaskAccessor.deleteAll(schemaName);
        //ddlEngineTaskAccessor.deleteAllArchive(schemaName);
        readWriteLockAccessor.deleteAll(schemaName);
        scheduledJobsAccessor.deleteAll(schemaName);
        firedScheduledJobsAccessor.deleteAll(schemaName);
    }

}
