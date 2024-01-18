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

import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.model.lifecycle.Lifecycle;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.rule.exception.TddlRuleException;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * tddl rule config管理
 *
 * @author jianghang 2013-11-5 下午3:34:36
 * @since 5.0.0
 */
public abstract class TddlRuleConfig extends AbstractLifecycle implements Lifecycle {

    protected static final Logger logger = LoggerFactory.getLogger(TddlRuleConfig.class);
    protected static final String NO_VERSION_NAME = "__VN__";
    protected String appName;
    protected String unitName;

    protected String appRuleString;


    /**
     * key = 0(old),1(new),2,3,4... value= version
     */
    protected volatile Map<String, VirtualTableRoot> vtrs = Maps.newLinkedHashMap();
    protected volatile Map<String, String> ruleStrs = Maps.newHashMap();
    protected volatile Map<Integer, String> versionIndex = Maps.newHashMap();

    private volatile Map<String, Map<String, Set<String>>> versionedTableNames = Maps.newConcurrentMap();

    protected boolean allowEmptyRule = false;
    // 默认物理的dbIndex，针对allowEmptyRule=true时有效
    protected String defaultDbIndex;
    // 规则最后一次变更的时间戳
    protected long lastTimestamp = System.currentTimeMillis();

    /**
     * <pre>
     * 返回当前使用的rule规则
     * 1. 如果是本地文件，则直接返回本地文件的版本 (本地文件存在多版本时，直接返回第一个版本)
     * 2. 如果是动态规则，则直接返回第一个版本
     *
     * ps. 正常情况，只有一个版本会处于使用中，也就是在数据库动态切换出现多版本使用中.
     * </pre>
     */
    public VirtualTableRoot getCurrentRule() {
        if (versionIndex.size() == 0) {
            if (!allowEmptyRule) {
                throw new TddlRuleException("规则对象为空!请检查是否存在规则!");
            } else {
                return null;
            }
        }

        return vtrs.get(versionIndex.get(0));
    }

    public VirtualTableRoot getVersionRule(String version) {
        VirtualTableRoot vtr = vtrs.get(version);
        if (vtr == null && !allowEmptyRule) {
            throw new TddlRuleException("规则对象为空!请检查是否存在规则!");
        }

        return vtr;
    }

    /**
     * 获取当前在用的版本，理论上正常只有一个版本(切换时出现两个版本)，顺序返回版本，第一个版本为当前正在使用中的旧版本
     */
    public List<String> getAllVersions() {
        int size = versionIndex.size();
        List<String> versions = Lists.newArrayList();
        for (int i = 0; i < size; i++) {
            versions.add(versionIndex.get(i));
        }

        return versions;
    }

    public Map<String, String> getCurrentRuleStrMap() {
        return ruleStrs;
    }

    public Map<String, Map<String, Set<String>>> getVersionedTableNames() {
        return versionedTableNames;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public void setUnitName(String unitName) {
        this.unitName = unitName;
    }

    public void setAllowEmptyRule(boolean allowEmptyRule) {
        this.allowEmptyRule = allowEmptyRule;
    }

    public void setDefaultDbIndex(String defaultDbIndex) {
        this.defaultDbIndex = defaultDbIndex;
    }

    public boolean isAllowEmptyRule() {
        return allowEmptyRule;
    }

    public String getDefaultDbIndex() {
        return defaultDbIndex;
    }

    public String getAppName() {
        return appName;
    }

    public long getLastTimestamp() {
        return lastTimestamp;
    }

    public String getAppRuleString() {
        return appRuleString;
    }

    public void setAppRuleString(String appRuleString) {
        this.appRuleString = appRuleString;
    }
}
