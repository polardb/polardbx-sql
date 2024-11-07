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

package com.alibaba.polardbx.gms.topology;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import com.alibaba.polardbx.gms.listener.impl.MetaDbDataIdBuilder;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @author youtianyu
 */
public class VariableConfigAccessor extends AbstractAccessor {
    private static final Logger logger = LoggerFactory.getLogger(VariableConfigAccessor.class);
    private static final String VARIABLE_CONFIG_TABLE = wrap(GmsSystemTables.VARIABLE_CONFIG);
    public static final String READONLY = "READONLY";
    private static final String SELECT_ALL = "select * from " + VARIABLE_CONFIG_TABLE;
    private static final String SELECT_BY_PARAM_KEY_AND_INST_ID = SELECT_ALL + " where param_key=? and inst_id=?";
    private static final String UPDATE_PARAM_VALUE =
        "replace into " + VARIABLE_CONFIG_TABLE + "set param_val=?, param_key=?, inst_id=?";
    private static final String INSERT_IGNORE_VARIABLE_CONFIGS =
        "insert ignore into " + VARIABLE_CONFIG_TABLE
            + " (inst_id, param_key, param_val, extra) values (?, ?, ?, ?)";

    public void addVariableConfigs(List<VariableConfigRecord> variableConfigRecords) {
        try (PreparedStatement preparedStatement = this.connection.prepareStatement(INSERT_IGNORE_VARIABLE_CONFIGS)) {
            for (VariableConfigRecord variableConfigRecord : variableConfigRecords) {
                preparedStatement.setString(1, variableConfigRecord.instId);
                preparedStatement.setString(2, variableConfigRecord.paramKey);
                preparedStatement.setString(3, variableConfigRecord.paramValue);
                preparedStatement.setString(4, variableConfigRecord.extra);
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
        } catch (Throwable e) {
            logger.error("Failed to query the system table '" + VARIABLE_CONFIG_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                VARIABLE_CONFIG_TABLE, e.getMessage());
        }
    }

    public void addVariableConfigs(String instId, Properties properties, boolean notify) {
        try (PreparedStatement preparedStatement = this.connection.prepareStatement(INSERT_IGNORE_VARIABLE_CONFIGS)) {
            for (String propName : properties.stringPropertyNames()) {
                String propVal = properties.getProperty(propName);
                preparedStatement.setString(1, instId);
                preparedStatement.setString(2, propName);
                preparedStatement.setString(3, propVal);
                preparedStatement.setString(4, "");
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
        } catch (Throwable e) {
            logger.error("Failed to query the system table '" + VARIABLE_CONFIG_TABLE + "'", e);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e, "query",
                VARIABLE_CONFIG_TABLE, e.getMessage());
        }

        if (notify) {
            MetaDbConfigManager.getInstance().notify(MetaDbDataIdBuilder.getVariableConfigDataId(instId), connection);
        }
    }

    private List<VariableConfigRecord> queryBySql(String sql, Map<Integer, ParameterContext> params) {
        try {
            return MetaDbUtil.query(sql, params, VariableConfigRecord.class
                , connection);
        } catch (Throwable t) {
            logger.error("Failed to query system table " + VARIABLE_CONFIG_TABLE + " sql: " + sql, t);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, t, "query", VARIABLE_CONFIG_TABLE,
                t.getMessage());
        }
    }

    public List<VariableConfigRecord> queryAll() {
        return queryBySql(SELECT_ALL, null);
    }

    public List<VariableConfigRecord> queryByParamKey(String paramKey, String instId) {
        Map<Integer, ParameterContext> params = new HashMap<>();
        MetaDbUtil.setParameter(1, params, ParameterMethod.setString, paramKey);
        MetaDbUtil.setParameter(2, params, ParameterMethod.setString, instId);
        return queryBySql(SELECT_BY_PARAM_KEY_AND_INST_ID, params);
    }

    public int[] updateBySql(String sql, List<Map<Integer, ParameterContext>> paramsList) {
        try {
            return MetaDbUtil.update(sql, paramsList, connection);
        } catch (Throwable t) {
            logger.error("Failed to update system table " + VARIABLE_CONFIG_TABLE + " sql: " + sql, t);
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, t, "update", VARIABLE_CONFIG_TABLE,
                t.getMessage());
        }
    }

    public int[] updateParamsValue(Properties props, String instId) {
        List<Map<Integer, ParameterContext>> paramsList = new LinkedList<>();
        for (String paramKey : props.stringPropertyNames()) {
            Map<Integer, ParameterContext> params = new HashMap<>();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, props.getProperty(paramKey));
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, paramKey);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setString, instId);
            paramsList.add(params);
        }
        int[] updateResult = updateBySql(UPDATE_PARAM_VALUE, paramsList);
        MetaDbConfigManager.getInstance().notify(MetaDbDataIdBuilder.getVariableConfigDataId(instId), connection);
        return updateResult;
    }
}