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

package com.alibaba.polardbx.executor.ddl.twophase;

import com.alibaba.polardbx.gms.metadb.multiphase.MultiPhaseDdlInfoAccessor;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

public class TwoPhaseDdlReporter {
    public static Boolean acquireTwoPhaseDdlId(String schemaName, String logicalTableName, Long multiPhaseDdlId) {
        // TODO(2pc-ddl): insert into record.
        int affectedRows = 1;
        try (Connection connection = MetaDbUtil.getConnection()) {
            MultiPhaseDdlInfoAccessor multiPhaseDdlInfoAccessor = new MultiPhaseDdlInfoAccessor();
            multiPhaseDdlInfoAccessor.setConnection(connection);
            affectedRows =
                multiPhaseDdlInfoAccessor.insertIgnoreMultiPhaseDdlId(multiPhaseDdlId, schemaName, logicalTableName);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return affectedRows > 0;
    }

    public Boolean updateStats(String phase) {
        return true;
    }

    public Map<String, Set<String>> fetchEmittedPhyTables() {
        Map<String, Set<String>> objectObjectHashMap = new HashMap<>();
        return objectObjectHashMap;
    }

    public Boolean appendEmitPhyTable(String phyTable) {
        return true;
    }

    public Boolean collectStatsAndUpdateState(AtomicReference<String> state) {
        return true;
    }
}
