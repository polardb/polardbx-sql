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

package com.alibaba.polardbx.gms.util;

import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.eventlogger.EventType;
import com.alibaba.polardbx.gms.topology.CreateDbInfo;
import com.alibaba.polardbx.gms.topology.DbInfoRecord;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;

import java.util.List;

public class DbEventUtil {
    public static void logFirstAutoDbCreationEvent(CreateDbInfo createDbInfo) {
        try {
            if (createDbInfo.getDbType() != DbInfoRecord.DB_TYPE_NEW_PART_DB) {
                return;
            }
            List<DbInfoRecord> dbRecords = DbTopologyManager.getNewPartDbInfoFromMetaDb();
            if (dbRecords.size() > 1) {
                return;
            }
            String dbName = createDbInfo.getDbName();
            EventLogger.log(EventType.CREATE_AUTO_MODE_DB,
                String.format("Find first auto-mode database[%s] created", dbName));
        } catch (Throwable ex) {
            MetaDbLogUtil.META_DB_LOG.error(ex);
        }
    }

    public static void logStandardToEnterpriseEditionEvent(String logicalDb, String phyDb) {
        EventLogger.log(EventType.STANDARD_TO_ENTERPRISE,
            String.format("standard database [%s] converted to enterprise edition database [%s]", logicalDb, phyDb));
    }
}