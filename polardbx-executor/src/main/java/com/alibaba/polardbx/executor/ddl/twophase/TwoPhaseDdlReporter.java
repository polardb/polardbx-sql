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
