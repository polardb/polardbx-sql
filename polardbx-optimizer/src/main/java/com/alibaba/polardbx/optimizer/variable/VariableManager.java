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
import org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static com.alibaba.polardbx.common.TddlConstants.SQL_MODE;

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
     * Whether sql_mode is strict for create-table or create-index. If sql_mode contains "STRICT_ALL_TABLES" or
     * "STRICT_TRANS_TABLES", it is strict.
     *
     * @param ec execution context, which contains user-specified server variables and sql_mode
     * @return whether sql_mode is strict for create-table or create-index
     */
    public boolean isSqlModeStrict(ExecutionContext ec) {
        String sqlMode = null;
        // Use user-specified value first.
        if (null != ec && null != ec.getSqlMode() && !StringUtils.equalsIgnoreCase(ec.getSqlMode(), "default")) {
            sqlMode = ec.getSqlMode();
        }
        // Then, consider using cached value if we can not get it from execution context.
        if (null == sqlMode && this.sessionVariablesCache.containsKey(SQL_MODE)) {
            final Object obj = this.getSessionVariable(SQL_MODE);
            sqlMode = obj == null ? null : (String) obj;
        }
        return StringUtils.containsIgnoreCase(sqlMode, "STRICT_ALL_TABLES")
            || StringUtils.containsIgnoreCase(sqlMode, "STRICT_TRANS_TABLES");
    }

}
