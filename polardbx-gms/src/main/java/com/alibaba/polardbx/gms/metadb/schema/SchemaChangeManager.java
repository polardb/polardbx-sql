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

package com.alibaba.polardbx.gms.metadb.schema;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.logger.LoggerInit;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SchemaChangeManager extends AbstractLifecycle {

    private static final Logger LOGGER = LoggerInit.TDDL_DYNAMIC_CONFIG;

    static final String TEST_TABLE_PREFIX = "_meta_db_test_";

    private static final int VERSION_OFFSET = 2;

    private static final SchemaChangeManager INSTANCE = new SchemaChangeManager();

    private final Map<String, Integer> TABLE_SCHEMA_CHANGES = new ConcurrentHashMap<>();

    private SchemaChangeAccessor schemaChangeAccessor;
    private SchemaChangeBuilder schemaChangeBuilder;

    private SchemaChangeManager() {
        this.schemaChangeAccessor = new SchemaChangeAccessor();
        this.schemaChangeBuilder = new SchemaChangeBuilder();
    }

    public static SchemaChangeManager getInstance() {
        INSTANCE.init();
        return INSTANCE;
    }

    @Override
    protected void doInit() {
        super.doInit();
        schemaChangeAccessor.init();
        schemaChangeBuilder.init();
    }

    public void handle() {
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            schemaChangeAccessor.setConnection(metaDbConn);

            boolean locked = false;

            boolean schemaChangeRequired = doSchemaChange(false);

            while (!locked && schemaChangeRequired) {
                locked = schemaChangeAccessor.getLock();
                if (!locked) {
                    schemaChangeRequired = doSchemaChange(false);
                }
            }

            if (locked && schemaChangeRequired) {
                doSchemaChange(true);
            }

            if (locked) {
                schemaChangeAccessor.releaseLock();
            }
        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_GET_CONNECTION, e,
                "schema change lock: " + e.getMessage());
        } finally {
            schemaChangeAccessor.setConnection(null);
        }
    }

    private boolean doSchemaChange(boolean lockGot) {
        boolean schemaChangeRequired = false;

        Map<String, Pair<String, List<String>>> allSchemaChanges =
            schemaChangeBuilder.getSchemaChanges();

        if (allSchemaChanges != null && allSchemaChanges.size() > 0) {
            long totalTime = 0L;

            loadCurrentVersions();

            for (Map.Entry<String, Pair<String, List<String>>> entry : allSchemaChanges.entrySet()) {
                String systemTableName = entry.getKey();
                String fullCreateTableStmt = entry.getValue().getKey();
                List<String> schemaChangeStmtsInOrder = entry.getValue().getValue();

                Pair<Boolean, Long> result =
                    register(systemTableName, fullCreateTableStmt, schemaChangeStmtsInOrder, lockGot);

                schemaChangeRequired = schemaChangeRequired || result.getKey();
                totalTime += result.getValue();
            }

            LOGGER.info("Totally spent " + totalTime + "ms creating " + allSchemaChanges.size() + " system tables");
        }

        return schemaChangeRequired;
    }

    private Pair<Boolean, Long> register(String systemTableName, String fullCreateTableStmt,
                                         List<String> schemaChangeStmtsInOrder, boolean lockGot) {
        long startTime = System.currentTimeMillis();
        long endTime = startTime;

        if (!validate(systemTableName, fullCreateTableStmt)) {
            return new Pair<>(Boolean.FALSE, endTime - startTime);
        }

        int schemaChangeCount = schemaChangeStmtsInOrder != null ? schemaChangeStmtsInOrder.size() : 0;

        int nextVersion = 1;

        // Get the table's current version.
        Integer currentVersion = TABLE_SCHEMA_CHANGES.get(systemTableName);

        // Check if the system table exists.
        boolean systemTableExists = schemaChangeAccessor.check(systemTableName);

        if (currentVersion != null && systemTableExists) {
            nextVersion = currentVersion + 1;
        }

        if (nextVersion > (schemaChangeCount + 1)) {
            endTime = System.currentTimeMillis();
            // Don't need to change current schema.
            return new Pair<>(Boolean.FALSE, endTime - startTime);
        }

        if (nextVersion == 1 && lockGot) {
            // We need to update a final version since this is a full statement.
            int finalVersion = nextVersion + schemaChangeCount;

            // Execute the initial CREATE TABLE statement.
            schemaChangeAccessor.execute(fullCreateTableStmt);

            if (currentVersion != null) {
                // The system table may be dropped accidentally and the schema
                // change record still exists, so we can update it.
                schemaChangeAccessor.update(systemTableName, finalVersion);
            } else {
                // This is initial table creation, so we should insert a record.
                schemaChangeAccessor.insert(systemTableName, finalVersion);
            }

            // Update cache.
            TABLE_SCHEMA_CHANGES.put(systemTableName, finalVersion);

            endTime = System.currentTimeMillis();

            // Return since we have completed the full statement.
            return new Pair<>(Boolean.TRUE, endTime - startTime);
        }

        if (lockGot) {
            // Execute the schema change statements in order.
            for (; nextVersion < (schemaChangeCount + VERSION_OFFSET); nextVersion++) {
                // Execute current schema change statement.
                schemaChangeAccessor.execute(schemaChangeStmtsInOrder.get(nextVersion - VERSION_OFFSET));

                // This is a schema change, so we should update current version.
                schemaChangeAccessor.update(systemTableName, nextVersion);

                // Update cache as well.
                TABLE_SCHEMA_CHANGES.put(systemTableName, nextVersion);
            }
        }

        endTime = System.currentTimeMillis();

        return new Pair<>(Boolean.TRUE, endTime - startTime);
    }

    private boolean validate(String systemTableName, String fullCreateTableStmt) {
        // Only the leader should handle schema changes.
        if (schemaChangeAccessor == null || schemaChangeAccessor.getConnection() == null) {
            return false;
        }

        boolean isValidTable = GmsSystemTables.contains(systemTableName) ||
            TStringUtil.startsWithIgnoreCase(systemTableName, TEST_TABLE_PREFIX);

        if (TStringUtil.isBlank(systemTableName) || !isValidTable) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_CHECK_ARGUMENTS,
                "Please register a valid system table name. Invalid/unregistered name: " + systemTableName);
        }

        if (TStringUtil.isBlank(fullCreateTableStmt)) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_CHECK_ARGUMENTS,
                "Please provide at lease one full CREATE TABLE statement for system table '" + systemTableName
                    + "'. If there are multiple schema change statements on the table, "
                    + "please put them into the list in the order of schema changes");
        }

        return true;
    }

    private void loadCurrentVersions() {
        // Load all tables' current versions.
        List<SchemaChangeRecord> records = schemaChangeAccessor.query();
        if (records != null && !records.isEmpty()) {
            for (SchemaChangeRecord record : records) {
                TABLE_SCHEMA_CHANGES.put(record.getTableName(), record.getVersion());
            }
        }
    }

    @Override
    protected void doDestroy() {
        super.doDestroy();
        schemaChangeAccessor.destroy();
        schemaChangeBuilder.destroy();
        TABLE_SCHEMA_CHANGES.clear();
    }

    /**
     * For test only
     */
    void setSchemaChangeBuilder(SchemaChangeBuilder schemaChangeBuilder) {
        this.schemaChangeBuilder = schemaChangeBuilder;
    }

}

