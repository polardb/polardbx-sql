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

package com.alibaba.polardbx.optimizer.variable;

import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.optimizer.biv.MockUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author dylan
 */
public class VariableManager extends AbstractLifecycle {

    private static final Logger logger = LoggerFactory.getLogger(VariableManager.class);

    private final String schemaName;

    private final ParamManager paramManager;

    private final IVariableProxy variableProxy;

    AtomicLong lastSwapTime;

    volatile ImmutableMap<String, Object> sessionVariablesCache;

    volatile ImmutableMap<String, Object> globalVariablesCache;

    public VariableManager(String schemaName,
                           IVariableProxy variableProxy,
                           Map<String, Object> connectionProperties) {
        this.schemaName = schemaName;
        this.paramManager = new ParamManager(connectionProperties);
        this.variableProxy = variableProxy;
    }

    @Override
    protected void doInit() {
        super.doInit();
        if (ConfigDataMode.isFastMock()) {
            sessionVariablesCache = ImmutableMap.<String, Object>builder().build();
            globalVariablesCache = ImmutableMap.<String, Object>builder().build();
            lastSwapTime = new AtomicLong(System.nanoTime());
            return;
        }
        sessionVariablesCache = variableProxy.getSessionVariables();
        globalVariablesCache = variableProxy.getGlobalVariables();
        lastSwapTime = new AtomicLong(System.nanoTime());
    }

    @Override
    protected void doDestroy() {
        super.doDestroy();
    }

    public IVariableProxy getVariableProxy() {
        return variableProxy;
    }

    private void checkExpireTime() {
        long expireTime = paramManager.getLong(ConnectionParams.VARIABLE_EXPIRE_TIME) * 1000 * 1000;
        long now = System.nanoTime();
        long tmpLastSwapTime = lastSwapTime.get();
        if (now - tmpLastSwapTime > expireTime) {
            if (lastSwapTime.compareAndSet(tmpLastSwapTime, now)) {
                sessionVariablesCache = variableProxy.getSessionVariables();
                globalVariablesCache = variableProxy.getGlobalVariables();
            } else {
                assert now - lastSwapTime.get() <= expireTime;
            }
        }
    }

    public Object getSessionVariable(String key) {
        return getVariable(key, sessionVariablesCache);
    }

    public Object getGlobalVariable(String key) {
        return getVariable(key, globalVariablesCache);
    }

    Object getVariable(String key, ImmutableMap<String, Object> m) {
        if (key == null) {
            return null;
        }
        if (ConfigDataMode.isFastMock()) {
            return MockUtils.mockVariable(key);
        }
        key = key.toLowerCase();
        Object value = m.get(key);
        checkExpireTime();
        return value;
    }

    public void invalidateAll() {
        sessionVariablesCache = variableProxy.getSessionVariables();
        globalVariablesCache = variableProxy.getGlobalVariables();
        lastSwapTime.set(System.currentTimeMillis());
    }

    /**
     * Get the server variable character_set_database, if user changes this variable, this change can be notified
     * using ExecutionContext.getServerVariables().
     *
     * @param ec execution context, which contains user-specified server variables
     * @return server variable: character_set_database
     */
    public String getCharsetDatabase(ExecutionContext ec) {
        if (null != ec.getServerVariables()) {
            if (ec.getServerVariables().containsKey("character_set_database")) {
                return (String) ec.getServerVariables().get("character_set_database");
            }
        }
        if (sessionVariablesCache.containsKey("character_set_database")) {
            return (String) this.sessionVariablesCache.get("character_set_database");
        }
        return null;
    }

    /**
     * Whether sql_mode is strict for create-table or create-index. If sql_mode contains "STRICT_ALL_TABLES" or
     * "STRICT_TRANS_TABLES", it is strict.
     *
     * @param ec execution context, which contains user-specified server variables and sql_mode
     * @return whether sql_mode is strict for create-table or create-index
     */
    public boolean isSqlModeStrict(ExecutionContext ec) {
        if (null != ec && ec.getServerVariables().containsKey("sql_mode")
            && !StringUtils.equalsIgnoreCase((String) ec.getServerVariables().get("sql_mode"), "default")) {
            if (ec.getServerVariables().get("sql_mode") == null) {
                return false;
            }
            return ((String) ec.getServerVariables().get("sql_mode")).toUpperCase().contains("STRICT_ALL_TABLES")
                || ((String) ec.getServerVariables().get("sql_mode")).toUpperCase().contains("STRICT_TRANS_TABLES");
        }
        if (this.sessionVariablesCache.containsKey("sql_mode")) {
            return ((String) this.sessionVariablesCache.get("sql_mode")).toUpperCase().contains("STRICT_ALL_TABLES")
                || ((String) this.sessionVariablesCache.get("sql_mode")).toUpperCase().contains("STRICT_TRANS_TABLES");
        }
        return false;
    }

    /**
     * Return max key length the database support, it is either 767 bytes or 3072 bytes.
     * For mysql 5.7, it depend on "innodb_large_prefix"; for mysql 8.0, it depend on "row_format".
     * Length of column in an index can't exceed max key length.
     *
     * @param create a Sql node for create-table or create-index or add-index
     * @return max key length, 767 bytes or 3072 bytes, depending on some server variables
     */
    public int getMaxKeyLen(SqlCreate create) {
        final int largeKeyLen = 3072;
        final int nonLargeKeyLen = 767;
        if (this.sessionVariablesCache.containsKey("version") &&
            ((String) this.sessionVariablesCache.get("version")).startsWith("5.7")) {
            // for mysql5.7, max key length depend on "innodb_large_prefix"
            if (this.sessionVariablesCache.containsKey("innodb_large_prefix")) {
                final String innodbLargePrefix = ((String) this.sessionVariablesCache.get("innodb_large_prefix"));
                if (StringUtils.equalsIgnoreCase(innodbLargePrefix, "ON")) {
                    return largeKeyLen;
                } else if (StringUtils.equalsIgnoreCase(innodbLargePrefix, "OFF")) {
                    return nonLargeKeyLen;
                }
            }
        } else if (this.sessionVariablesCache.containsKey("version") &&
            ((String) this.sessionVariablesCache.get("version")).startsWith("8.0")) {
            // for mysql 8.0, max key length depend on "row_format"
            if (create instanceof SqlCreateTable) {
                // if user specifies row_format in create-table statement
                final String userSpecifiedRowFormat = ((SqlCreateTable) create).getRowFormat();
                if (null != userSpecifiedRowFormat && !StringUtils
                    .equalsIgnoreCase(userSpecifiedRowFormat, "default")) {
                    if (!StringUtils.equalsIgnoreCase(userSpecifiedRowFormat, "dynamic")
                        && !StringUtils.equalsIgnoreCase(userSpecifiedRowFormat, "compressed")) {
                        return nonLargeKeyLen;
                    } else {
                        return largeKeyLen;
                    }
                } else {
                    // if user do not specify, use default row_format
                    if (this.sessionVariablesCache.containsKey("innodb_default_row_format")) {
                        final String innodbDefaultRowFormat =
                            ((String) this.sessionVariablesCache.get("innodb_default_row_format"));
                        if (!StringUtils.equalsIgnoreCase(innodbDefaultRowFormat, "dynamic")
                            && !StringUtils.equalsIgnoreCase(innodbDefaultRowFormat, "compressed")) {
                            return nonLargeKeyLen;
                        } else {
                            return largeKeyLen;
                        }
                    }
                }
            } else {
                /**
                 * 对于mysql8.0，当存在varchar等变长类型，row_format必须为dynamic或compressed，因此返回3072；
                 * 当为fixed时，由于表已创建，其中的char等定长字段本身就不允许超过767字节，索引中出现char等定长列就隐含着必定合法；
                 * 因此，返回3072用于判断varchar等变长列即可。
                 */
                return largeKeyLen;
            }
        }
        // if not the above cases, return large prefix key length and let the physical ddl to detect the error
        return largeKeyLen;
    }
}
