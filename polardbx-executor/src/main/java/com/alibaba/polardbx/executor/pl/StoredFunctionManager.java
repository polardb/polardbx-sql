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

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCreateFunctionStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SqlDataAccess;
import com.alibaba.polardbx.executor.ddl.job.task.basic.pl.PlConstants;
import com.alibaba.polardbx.executor.ddl.job.task.basic.pl.accessor.FunctionAccessor;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.pl.function.FunctionDefinitionRecord;
import com.alibaba.polardbx.gms.metadb.pl.function.FunctionMetaRecord;
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
public class StoredFunctionManager {
    private static final Logger logger = LoggerFactory.getLogger(StoredFunctionManager.class);

    public static StoredFunctionManager INSTANCE = new StoredFunctionManager();

    Map<String, Pair<SQLCreateFunctionStatement, Long>> functions = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    long totalSpace = MemorySetting.UNLIMITED_SIZE;

    long usedSpace = 0;

    public static StoredFunctionManager getInstance() {
        return INSTANCE;
    }

    private StoredFunctionManager() {
        initStoredFunctions();
    }

    public synchronized void register(String functionName) {
        if (functions.containsKey(functionName)) {
            return;
        }
        functions.put(functionName, PlConstants.FUNCTION_PLACE_HOLDER);
    }

    public synchronized void unregister(String functionName) {
        if (functions.containsKey(functionName)) {
            usedSpace -= functions.get(functionName).getValue();
        }
        functions.remove(functionName);
    }

    public synchronized SQLCreateFunctionStatement search(String functionName) {
        if (!functions.containsKey(functionName)) {
            return null;
        }
        SQLCreateFunctionStatement statement = functions.get(functionName).getKey();
        if (statement == PlConstants.FUNCTION_STMT_HOLDER) {
            statement = loadStoredFunction(functionName);
        }
        return statement;
    }

    public synchronized boolean containsFunction(String functionName) {
        return functions.containsKey(functionName);
    }

    private synchronized SQLCreateFunctionStatement loadStoredFunction(String functionName) {
        try (Connection connection = MetaDbUtil.getConnection()) {
            FunctionAccessor accessor = new FunctionAccessor();
            accessor.setConnection(connection);
            List<FunctionDefinitionRecord> records = accessor.getFunctionDefinition(functionName);
            if (records.size() > 0) {
                String createFunction = records.get(0).definition;
                SQLCreateFunctionStatement
                    createFunctionStatement =
                    (SQLCreateFunctionStatement) FastsqlUtils.parseSql(createFunction).get(0);
                createFunctionStatement.setSqlDataAccess(
                    records.get(0).canPush ? SqlDataAccess.NO_SQL : SqlDataAccess.CONTAINS_SQL);
                long functionSize = createFunction.getBytes().length;
                checkCapacity(functionSize);
                usedSpace += functionSize;
                functions.put(functionName, Pair.of(createFunctionStatement, functionSize));
                return createFunctionStatement;
            } else {
                return null;
            }
        } catch (SQLException ex) {
            throw new RuntimeException("Load function failed: " + ex);
        }
    }

    private synchronized void checkCapacity(long functionSize) {
        if (totalSpace - usedSpace < functionSize) {
            for (Map.Entry<String, Pair<SQLCreateFunctionStatement, Long>> entry : functions.entrySet()) {
                removeIfLoad(entry);
                if (totalSpace - usedSpace >= functionSize) {
                    break;
                }
            }
        }
    }

    private synchronized void initStoredFunctions() {
        if (MetaDbDataSource.getInstance() == null) {
            return;
        }
        try (Connection connection = MetaDbUtil.getConnection()) {
            FunctionAccessor accessor = new FunctionAccessor();
            accessor.setConnection(connection);
            List<FunctionMetaRecord> records = accessor.loadFunctionMetas();
            for (FunctionMetaRecord record : records) {
                String functionName = record.name;
                register(functionName);
            }
        } catch (SQLException ex) {
            logger.error("load function failed when init", ex);
            throw new RuntimeException("Load function failed: " + ex);
        }
        totalSpace = MemoryManager.getInstance().getFunctionCacheLimit();
    }

    public synchronized void reload() {
        functions.clear();
        usedSpace = 0;
        initStoredFunctions();
    }

    public synchronized void clearCache() {
        for (Map.Entry<String, Pair<SQLCreateFunctionStatement, Long>> entry : functions.entrySet()) {
            entry.setValue(PlConstants.FUNCTION_PLACE_HOLDER);
        }
        usedSpace = 0;
    }

    public synchronized void resizeCache(long totalSize) {
        no_enough_space:
        if (usedSpace > totalSize) {
            for (Map.Entry<String, Pair<SQLCreateFunctionStatement, Long>> entry : functions.entrySet()) {
                removeIfLoad(entry);
                if (usedSpace <= totalSize) {
                    break no_enough_space;
                }
            }
        }
        totalSpace = totalSize;
    }

    private synchronized void removeIfLoad(Map.Entry<String, Pair<SQLCreateFunctionStatement, Long>> functionPair) {
        if (functionPair.getValue().getKey() != PlConstants.FUNCTION_STMT_HOLDER) {
            usedSpace -= functionPair.getValue().getValue();
            functionPair.setValue(PlConstants.FUNCTION_PLACE_HOLDER);
        }
    }

    public synchronized Map<String, Long> getFunctions() {
        Map<String, Long> loadedFunctions = new HashMap<>();
        for (Map.Entry<String, Pair<SQLCreateFunctionStatement, Long>> entry : functions.entrySet()) {
            loadedFunctions.put(entry.getKey(), entry.getValue().getValue());
        }
        return loadedFunctions;
    }

    public synchronized long getUsedSize() {
        return usedSpace;
    }

    public synchronized long getTotalSize() {
        return totalSpace;
    }
}
