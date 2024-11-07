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

package com.alibaba.polardbx.executor.pl;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateProcedureStatement;
import com.alibaba.polardbx.executor.ddl.job.task.basic.pl.PlConstants;
import com.alibaba.polardbx.executor.ddl.job.task.basic.pl.accessor.ProcedureAccessor;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.pl.procedure.ProcedureDefinitionRecord;
import com.alibaba.polardbx.gms.metadb.pl.procedure.ProcedureMetaRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.memory.MemoryManager;
import com.alibaba.polardbx.optimizer.memory.MemorySetting;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * @author yuehan.wcf
 */
public class ProcedureManager {
    private static final Logger logger = LoggerFactory.getLogger(ProcedureManager.class);

    private volatile static ProcedureManager INSTANCE;

    /**
     * schema -> procedure name -> create procedure content
     */
    Map<String, Map<String, Pair<SQLCreateProcedureStatement, Long>>> procedures =
        new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    long totalSpace = MemorySetting.UNLIMITED_SIZE;

    long usedSpace = 0;

    public static ProcedureManager getInstance() {
        if (INSTANCE == null) {
            synchronized (ProcedureManager.class) {
                if (INSTANCE == null) {
                    INSTANCE = new ProcedureManager();
                    INSTANCE.initProcedures();
                }
            }
        }
        return INSTANCE;
    }

    private ProcedureManager() {
    }

    public synchronized void register(String schema, String procedureName) {
        procedures.putIfAbsent(schema, new TreeMap<>(String.CASE_INSENSITIVE_ORDER));
        procedures.get(schema).put(procedureName, PlConstants.PROCEDURE_PLACE_HOLDER);
    }

    public synchronized void unregister(String schema, String procedureName) {
        if (!procedures.containsKey(schema) || !procedures.get(schema).containsKey(procedureName)) {
            return;
        }
        long size = procedures.get(schema).get(procedureName).getValue();
        usedSpace -= size;
        procedures.get(schema).remove(procedureName);
    }

    public synchronized SQLCreateProcedureStatement search(String schema, String procedureName) {
        if (notFound(schema, procedureName)) {
            return null;
        }
        SQLCreateProcedureStatement statement = procedures.get(schema).get(procedureName).getKey();
        if (statement == PlConstants.PROCEDURE_STMT_HOLDER) {
            Pair<SQLCreateProcedureStatement, Long> stmtPair = loadProcedure(schema, procedureName);
            procedures.get(schema).put(procedureName, stmtPair);
            statement = stmtPair == null ? null : stmtPair.getKey();
        }
        return statement;
    }

    public synchronized boolean notFound(String schema, String procedureName) {
        return !procedures.containsKey(schema) || !procedures.get(schema).containsKey(procedureName);
    }

    private synchronized Pair<SQLCreateProcedureStatement, Long> loadProcedure(String schema, String procedureName) {
        try (Connection connection = MetaDbUtil.getConnection()) {
            if (connection == null) {
                logger.error("load procedure failed: " + schema + "@" + procedureName
                    + ", because get metaDb connection failed!");
                throw new TddlRuntimeException(ErrorCode.ERR_PROCEDURE_LOAD_FAILED, "Load procedure failed!");
            }
            ProcedureAccessor accessor = new ProcedureAccessor();
            accessor.setConnection(connection);
            List<ProcedureDefinitionRecord> records = accessor.getProcedureDefinition(schema, procedureName);
            if (records.size() == 0) {
                logger.error(String.format("procedure not found %s.%s", schema, procedureName));
                return null;
            } else {
                String definition = records.get(0).definition;
                long procedureSize = definition.getBytes().length;
                checkCapacity(procedureSize);
                usedSpace += procedureSize;
                return Pair.of((SQLCreateProcedureStatement) FastsqlUtils.parseSql(definition).get(0),
                    procedureSize);
            }
        } catch (SQLException ex) {
            logger.error("load procedure failed: " + schema + "@" + procedureName, ex);
            throw new TddlRuntimeException(ErrorCode.ERR_PROCEDURE_LOAD_FAILED, "Load procedure failed!");
        }
    }

    private synchronized void checkCapacity(long procedureSize) {
        no_enough_space:
        if (totalSpace - usedSpace < procedureSize) {
            // iterate schemas
            for (Map<String, Pair<SQLCreateProcedureStatement, Long>> proc : procedures.values()) {
                // iterate procedures
                for (Map.Entry<String, Pair<SQLCreateProcedureStatement, Long>> entry : proc.entrySet()) {
                    removeIfLoad(entry);
                    if (totalSpace - usedSpace >= procedureSize) {
                        break no_enough_space;
                    }
                }
            }
        }
    }

    public synchronized void unregisterWholeDb(String schema) {
        if (procedures.containsKey(schema)) {
            for (Pair<SQLCreateProcedureStatement, Long> relatedProcs : procedures.get(schema).values()) {
                usedSpace -= relatedProcs.getValue();
            }
        }
        procedures.remove(schema);
    }

    private synchronized void initProcedures() {
        if (MetaDbDataSource.getInstance() == null) {
            return;
        }
        try (Connection connection = MetaDbUtil.getConnection()) {
            ProcedureAccessor accessor = new ProcedureAccessor();
            accessor.setConnection(connection);
            List<ProcedureMetaRecord> records = accessor.loadProcedureMetas();
            for (ProcedureMetaRecord record : records) {
                register(record.schema, record.name);
            }
        } catch (SQLException ex) {
            logger.error("load procedure failed when init", ex);
            throw new TddlRuntimeException(ErrorCode.ERR_PROCEDURE_LOAD_FAILED, "Load procedure failed!");
        }
        totalSpace = MemoryManager.getInstance().getProcedureCacheLimit();
    }

    public synchronized void clearCache() {
        for (Map<String, Pair<SQLCreateProcedureStatement, Long>> proc : procedures.values()) {
            for (Map.Entry<String, Pair<SQLCreateProcedureStatement, Long>> entry : proc.entrySet()) {
                entry.setValue(PlConstants.PROCEDURE_PLACE_HOLDER);
            }
        }
        usedSpace = 0;
    }

    public synchronized void reload() {
        procedures.clear();
        usedSpace = 0;
        initProcedures();
    }

    public synchronized void resizeCache(long totalSize) {
        no_enough_space:
        if (usedSpace > totalSize) {
            // iterate schemas
            for (Map<String, Pair<SQLCreateProcedureStatement, Long>> proc : procedures.values()) {
                // iterate procedures
                for (Map.Entry<String, Pair<SQLCreateProcedureStatement, Long>> entry : proc.entrySet()) {
                    removeIfLoad(entry);
                    if (usedSpace <= totalSize) {
                        break no_enough_space;
                    }
                }
            }
        }
        totalSpace = totalSize;
    }

    private synchronized void removeIfLoad(Map.Entry<String, Pair<SQLCreateProcedureStatement, Long>> procedurePair) {
        if (procedurePair.getValue().getKey() != PlConstants.PROCEDURE_STMT_HOLDER) {
            usedSpace -= procedurePair.getValue().getValue();
            procedurePair.setValue(PlConstants.PROCEDURE_PLACE_HOLDER);
        }
    }

    public synchronized Map<String, Map<String, Long>> getProcedureStatus() {
        Map<String, Map<String, Long>> loadedProcedures = new HashMap<>();
        for (Map.Entry<String, Map<String, Pair<SQLCreateProcedureStatement, Long>>> proc : procedures.entrySet()) {
            String schema = proc.getKey();
            loadedProcedures.putIfAbsent(schema, new HashMap<>());
            for (Map.Entry<String, Pair<SQLCreateProcedureStatement, Long>> entry : proc.getValue().entrySet()) {
                String procedure = entry.getKey();
                Long size = entry.getValue().getValue();
                loadedProcedures.get(schema).put(procedure, size);
            }
        }
        return loadedProcedures;
    }

    public synchronized long getUsedSize() {
        return usedSpace;
    }

    public synchronized long getTotalSize() {
        return totalSpace;
    }

    public synchronized long getProcedureSize() {
        return procedures.values().stream().mapToLong(Map::size).sum();
    }
}
