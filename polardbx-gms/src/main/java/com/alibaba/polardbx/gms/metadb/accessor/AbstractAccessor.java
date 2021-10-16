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

package com.alibaba.polardbx.gms.metadb.accessor;

import com.google.common.collect.Maps;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.record.SystemTableRecord;
import com.alibaba.polardbx.gms.util.DdlMetaLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class AbstractAccessor extends AbstractLifecycle {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractAccessor.class);

    protected static final String COMMA = ",";
    protected static final String SINGLE_QUOTE = "'";

    protected Connection connection;

    public Connection getConnection() {
        return connection;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    protected static String wrap(String identifier) {
        return "`" + identifier + "`";
    }

    protected String concat(List<String> names) {
        if (names == null || names.isEmpty()) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        for (String name : names) {
            sb.append(COMMA).append(SINGLE_QUOTE).append(name).append(SINGLE_QUOTE);
        }
        return sb.deleteCharAt(0).toString();
    }

    protected int insert(String insertSql, String systemTable, Map<Integer, ParameterContext> params) {
        try {
            if (DdlMetaLogUtil.isDdlTable(systemTable)) {
                DdlMetaLogUtil.logSql(insertSql, params);
            }
            return MetaDbUtil.insert(insertSql, params, connection);
        } catch (SQLException e) {
            LOGGER.error("Failed to insert a new record into " + systemTable, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "insert into",
                systemTable,
                e.getMessage());
        }
    }

    protected int[] insert(String insertSql, String systemTable, List<Map<Integer, ParameterContext>> paramsBatch) {
        try {
            if (DdlMetaLogUtil.isDdlTable(systemTable)) {
                DdlMetaLogUtil.logSql(insertSql, paramsBatch);
            }
            return MetaDbUtil.insert(insertSql, paramsBatch, connection);
        } catch (SQLException e) {
            LOGGER.error("Failed to insert a batch of new records into " + systemTable, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "batch insert into",
                systemTable,
                e.getMessage());
        }
    }

    protected <T extends SystemTableRecord> List<T> query(String selectSql, String systemTable, Class clazz,
                                                          String schemaName) {
        return query(selectSql, systemTable, clazz, schemaName, null);
    }

    public <T extends SystemTableRecord> List<T> query(String selectSql, String systemTable, Class clazz,
                                                       String schemaName, String objectName) {
        return query(selectSql, systemTable, clazz, schemaName, objectName, connection);
    }

    protected <T extends SystemTableRecord> List<T> query(String selectSql, String systemTable, Class clazz,
                                                          String schemaName, String objectName, String objectName2) {
        return query(selectSql, systemTable, clazz, schemaName, objectName, objectName2, connection);
    }

    protected <T extends SystemTableRecord> List<T> query(String selectSql, String systemTable, Class clazz,
                                                          String schemaName, String objectName, DataSource dataSource) {
        try (Connection phyDbConn = dataSource.getConnection()) {
            return query(selectSql, systemTable, clazz, schemaName, objectName, phyDbConn);
        } catch (Exception e) {
            LOGGER.error("Failed to get connection for " + systemTable, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "get connection",
                systemTable,
                e.getMessage());
        }
    }

    protected <T extends SystemTableRecord> List<T> query(String selectSql, String systemTable, Class clazz,
                                                          long objectId) {
        try {
            Map<Integer, ParameterContext> params = Maps.newHashMap();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, objectId);
            return MetaDbUtil.query(selectSql, params, clazz, connection);
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + systemTable, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                systemTable,
                e.getMessage());
        }
    }

    private <T extends SystemTableRecord> List<T> query(String selectSql, String systemTable, Class clazz,
                                                        String schemaName, String objectName, Connection connection) {
        try {
            Map<Integer, ParameterContext> params;
            if (TStringUtil.isBlank(objectName)) {
                params = MetaDbUtil.buildStringParameters(new String[] {schemaName});
            } else {
                params = MetaDbUtil.buildStringParameters(new String[] {schemaName, objectName});
            }
            return MetaDbUtil.query(selectSql, params, clazz, connection);
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + systemTable, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                systemTable,
                e.getMessage());
        }
    }

    private <T extends SystemTableRecord> List<T> query(String selectSql, String systemTable, Class clazz,
                                                        String schemaName, String objectName, String objectName2,
                                                        Connection connection) {
        try {
            Map<Integer, ParameterContext> params;
            params = MetaDbUtil.buildStringParameters(new String[] {schemaName, objectName, objectName2});
            return MetaDbUtil.query(selectSql, params, clazz, connection);
        } catch (Exception e) {
            LOGGER.error("Failed to query the system table " + systemTable, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                systemTable,
                e.getMessage());
        }
    }

    protected int update(String updateSql, String systemTable, Map<Integer, ParameterContext> params) {
        try {
            if (DdlMetaLogUtil.isDdlTable(systemTable)) {
                DdlMetaLogUtil.logSql(updateSql, params);
            }
            return MetaDbUtil.update(updateSql, params, connection);
        } catch (SQLException e) {
            LOGGER.error("Failed to update an existing record in " + systemTable, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "update",
                systemTable,
                e.getMessage());
        }
    }

    protected int[] update(String updateSql, String systemTable, List<Map<Integer, ParameterContext>> paramsBatch) {
        try {
            if (DdlMetaLogUtil.isDdlTable(systemTable)) {
                DdlMetaLogUtil.logSql(updateSql, paramsBatch);
            }
            return MetaDbUtil.update(updateSql, paramsBatch, connection);
        } catch (SQLException e) {
            LOGGER.error("Failed to update a batch of existing records in " + systemTable, e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "batch update",
                systemTable,
                e.getMessage());
        }
    }

    protected int update(String updateSql, String systemTable, String schemaName, String objectName, long newValue) {
        return update(updateSql, systemTable, schemaName, objectName, String.valueOf(newValue), false);
    }

    protected int update(String updateSql, String systemTable, String schemaName, String objectName, String newValue) {
        return update(updateSql, systemTable, schemaName, objectName, newValue, true);
    }

    private int update(String updateSql, String systemTable, String schemaName, String objectName, String newValue,
                       Boolean isNewString) {
        try {
            int index = 0;
            Map<Integer, ParameterContext> params = new HashMap<>(2);
            if (isNewString != null) {
                if (isNewString) {
                    MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, newValue);
                } else {
                    MetaDbUtil.setParameter(++index, params, ParameterMethod.setLong, Long.valueOf(newValue));
                }
            }
            MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, schemaName);
            MetaDbUtil.setParameter(++index, params, ParameterMethod.setString, objectName);
            if (DdlMetaLogUtil.isDdlTable(systemTable)) {
                DdlMetaLogUtil.logSql(updateSql, params);
            }
            return MetaDbUtil.update(updateSql, params, connection);
        } catch (SQLException e) {
            LOGGER.error(
                "Failed to update " + systemTable + " with new value " + newValue + " for table " + wrap(objectName),
                e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "update",
                systemTable,
                e.getMessage());
        }
    }

    protected int delete(String deleteSql, String systemTable, String schemaName) {
        return delete(deleteSql, systemTable, schemaName, null);
    }

    protected int delete(String deleteSql, String systemTable, String schemaName, String objectName) {
        try {
            Map<Integer, ParameterContext> params;
            if (TStringUtil.isBlank(objectName)) {
                params = MetaDbUtil.buildStringParameters(new String[] {schemaName});
            } else {
                params = MetaDbUtil.buildStringParameters(new String[] {schemaName, objectName});
            }
            if (DdlMetaLogUtil.isDdlTable(systemTable)) {
                DdlMetaLogUtil.logSql(deleteSql, params);
            }
            return MetaDbUtil.delete(deleteSql, params, connection);
        } catch (SQLException e) {
            LOGGER.error(
                "Failed to delete from " + systemTable + " for table " + wrap(objectName), e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "delete from",
                systemTable,
                e.getMessage());
        }
    }

    private static final int ERROR_CODE_DUP_ENTRY = 1062;
    private static final String SQLSTATE_DUP_ENTRY = "23000";

    protected boolean checkIfDuplicate(SQLException e) {
        return ERROR_CODE_DUP_ENTRY == e.getErrorCode() &&
            TStringUtil.equalsIgnoreCase(SQLSTATE_DUP_ENTRY, e.getSQLState());
    }

}
