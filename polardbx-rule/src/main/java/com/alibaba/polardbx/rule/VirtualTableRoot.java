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

package com.alibaba.polardbx.rule;

import com.alibaba.polardbx.common.model.DBType;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.model.lifecycle.Lifecycle;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.TreeMaps;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.rule.utils.RuleUtils;

import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

/**
 * 一组{@linkplain TableRule}的集合
 *
 * @author junyu
 */
public class VirtualTableRoot extends AbstractLifecycle implements Lifecycle {

    private static final Logger logger = LoggerFactory.getLogger(VirtualTableRoot.class);
    protected String dbType = "MYSQL";
    protected Map<String, TableRule> virtualTableMap = TreeMaps.synchronizeMap();
    protected static Map<String, TableRule> testTableRuleMap = new TreeMap(String.CASE_INSENSITIVE_ORDER);
    protected Map<String, String> dbIndexMap = new TreeMap(String.CASE_INSENSITIVE_ORDER);

    protected String defaultDbIndex;
    protected boolean lazyInit = false;

    @Override
    public void doInit() {

        for (Map.Entry<String, TableRule> entry : virtualTableMap.entrySet()) {
            if (!isLazyInit()) {
                initTableRule(entry.getKey(), entry.getValue());
            }
        }
    }

    /**
     * 此处有个问题是Map中key对应的VirtualTableRule为null;
     */
    public TableRule getVirtualTable(String virtualTableName) {
        RuleUtils.notNull(virtualTableName, "virtual table name is null");
        if (testTableRuleMap.get(virtualTableName) != null) {
            return testTableRuleMap.get(virtualTableName);
        }
        TableRule tableRule = virtualTableMap.get(virtualTableName);
        if (tableRule != null && isLazyInit() && !tableRule.isInited()) {
            initTableRule(virtualTableName, tableRule);
        }

        return tableRule;
    }

    public Map<String, TableRule> getTableRules() {
        if (!ConfigDataMode.isFastMock()) {
            for (Map.Entry<String, TableRule> entry : virtualTableMap.entrySet()) {
                TableRule tableRule = entry.getValue();
                if (tableRule != null && isLazyInit() && !tableRule.isInited()) {
                    initTableRule(entry.getKey(), tableRule);
                }
            }
        }
        return virtualTableMap;
    }

    public void setTableRules(Map<String, TableRule> virtualTableMap) {
        Map<String, TableRule> logicTableMap = TreeMaps.caseInsensitiveMap();
        for (Entry<String, TableRule> entry : virtualTableMap.entrySet()) {
            logicTableMap.put(entry.getKey(), entry.getValue());
        }
        this.virtualTableMap = logicTableMap;
    }

    public static void setTestRule(String tableName, TableRule tableRule) {
        if (tableRule != null && TStringUtil.isEmpty(tableRule.getVirtualTbName())) {
            tableRule.setVirtualTbName(tableName);
        }
        testTableRuleMap.put(tableName, tableRule);
    }

    private void initTableRule(String tableNameKey, TableRule tableRule) {
        if (logger.isDebugEnabled()) {
            logger.debug("virtual table start to init :" + tableNameKey);
        }
        if (tableRule.getDbType() == null) {
            // 如果虚拟表中dbType为null,那指定全局dbType
            tableRule.setDbType(this.getDbTypeEnumObj());
        }

        if (tableRule.getVirtualTbName() == null) {
            tableRule.setVirtualTbName(tableNameKey);
        }
        if (isLazyInit()) {
            tableRule.setLazyInit(true);
        }
        try {
            tableRule.init();
        } catch (Exception e) {
            logger.error("Virtual table failed to initialize :" + tableNameKey, e);
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Virtual table initialized :" + tableNameKey);
        }
    }

    public String getDefaultDbIndex() {
        return defaultDbIndex;
    }

    public void setDefaultDbIndex(String defaultDbIndex) {
        this.defaultDbIndex = defaultDbIndex;
    }

    public Map<String, String> getDbIndexMap() {
        return dbIndexMap;
    }

    public DBType getDbTypeEnumObj() {
        return DBType.valueOf(this.dbType);
    }

    public String getDbType() {
        return this.dbType;
    }

    public void setDbType(String dbType) {
        this.dbType = dbType;
    }

    public void setLazyInit(boolean lazyInit) {
        this.lazyInit = lazyInit;
    }

    public boolean isLazyInit() {
        return lazyInit;
    }

    @Override
    protected void doDestroy() {
        super.doDestroy();

        this.virtualTableMap.clear();
        this.dbIndexMap.clear();
    }

    public VirtualTableRoot copy() {
        VirtualTableRoot newVirtualTableRoot = new VirtualTableRoot();
        newVirtualTableRoot.dbIndexMap.putAll(this.getDbIndexMap());
        newVirtualTableRoot.dbType = this.dbType;
        newVirtualTableRoot.defaultDbIndex = this.defaultDbIndex;
        newVirtualTableRoot.virtualTableMap.putAll(this.getTableRules());
        newVirtualTableRoot.lazyInit = this.lazyInit;
        return newVirtualTableRoot;
    }
}
