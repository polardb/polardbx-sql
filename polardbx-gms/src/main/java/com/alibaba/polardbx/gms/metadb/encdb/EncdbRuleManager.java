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

package com.alibaba.polardbx.gms.metadb.encdb;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.listener.ConfigListener;
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import com.alibaba.polardbx.gms.listener.impl.MetaDbDataIdBuilder;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.privilege.PolarAccount;
import com.alibaba.polardbx.gms.topology.ConfigListenerAccessor;
import com.alibaba.polardbx.gms.util.MetaDbLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author pangzhaoxing
 */
public class EncdbRuleManager extends AbstractLifecycle {

    private static final Logger logger = LoggerFactory.getLogger(EncdbRuleManager.class);

    public static final String ENCDB_SPREADED_RULE_ = "encdb_spreaded_rule_";

    private static final EncdbRuleManager INSTANCE = new EncdbRuleManager();

    private EncdbRuleMatchTree ruleMatchTree;

    public static EncdbRuleManager getInstance() {
        if (!INSTANCE.isInited()) {
            synchronized (INSTANCE) {
                if (!INSTANCE.isInited()) {
                    INSTANCE.init();
                }
            }
        }
        return INSTANCE;
    }

    @Override
    protected void doInit() {
        reloadEncRules();
        setupConfigListener();
    }

    private void setupConfigListener() {
        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            EncdbRuleConfigListener listener = new EncdbRuleConfigListener();
            MetaDbConfigManager.getInstance().register(MetaDbDataIdBuilder.ENCDB_RULE_DATA_ID, conn);
            MetaDbConfigManager.getInstance().bindListener(MetaDbDataIdBuilder.ENCDB_RULE_DATA_ID, listener);
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                "setup encdb rule config_listener failed");
        }
    }

    private void reloadEncRules() {
        try (Connection conn = MetaDbUtil.getConnection()) {
            EncdbRuleAccessor accessor = new EncdbRuleAccessor();
            accessor.setConnection(conn);
            EncdbRuleMatchTree newRuleMatchTree = new EncdbRuleMatchTree();
            for (EncdbRule rule : accessor.queryAllEnabledRules()) {
                newRuleMatchTree.insertRule(rule);
            }
            this.ruleMatchTree = newRuleMatchTree;
        } catch (SQLException e) {
            MetaDbLogUtil.META_DB_LOG.error(e);
            throw GeneralUtil.nestedException(e);
        }
    }

    public void insertEncRules(List<EncdbRule> rules) {
        if (rules == null || rules.isEmpty()) {
            return;
        }
        try (Connection conn = MetaDbUtil.getConnection()) {
            try {
                EncdbRuleAccessor accessor = new EncdbRuleAccessor();
                accessor.setConnection(conn);
                conn.setAutoCommit(false);
                for (EncdbRule rule : rules) {
                    accessor.insertRule(rule);
                }
                MetaDbConfigManager.getInstance().notify(MetaDbDataIdBuilder.ENCDB_RULE_DATA_ID, conn);
                conn.commit();
                // wait for all cn to load metadb
                MetaDbConfigManager.getInstance().sync(MetaDbDataIdBuilder.ENCDB_RULE_DATA_ID);
            } catch (SQLException e) {
                conn.rollback();
                throw GeneralUtil.nestedException(e);
            } finally {
                conn.setAutoCommit(true);
            }
        } catch (SQLException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public void replaceEncRules(List<EncdbRule> rules) {
        if (rules == null || rules.isEmpty()) {
            return;
        }
        try (Connection conn = MetaDbUtil.getConnection()) {
            try {
                EncdbRuleAccessor accessor = new EncdbRuleAccessor();
                accessor.setConnection(conn);
                conn.setAutoCommit(false);
                for (EncdbRule rule : rules) {
                    accessor.replaceRule(rule);
                }
                MetaDbConfigManager.getInstance().notify(MetaDbDataIdBuilder.ENCDB_RULE_DATA_ID, conn);
                conn.commit();
                // wait for all cn to load metadb
                MetaDbConfigManager.getInstance().sync(MetaDbDataIdBuilder.ENCDB_RULE_DATA_ID);
            } catch (SQLException e) {
                conn.rollback();
                throw GeneralUtil.nestedException(e);
            } finally {
                conn.setAutoCommit(true);
            }
        } catch (SQLException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public int deleteEncRule(String ruleName) {
        try (Connection conn = MetaDbUtil.getConnection()) {
            EncdbRuleAccessor accessor = new EncdbRuleAccessor();
            accessor.setConnection(conn);
            int update = accessor.deleteRuleByName(ruleName);
            MetaDbConfigManager.getInstance().notify(MetaDbDataIdBuilder.ENCDB_RULE_DATA_ID, conn);
            // wait for all cn to load metadb
            MetaDbConfigManager.getInstance().sync(MetaDbDataIdBuilder.ENCDB_RULE_DATA_ID);
            return update;
        } catch (SQLException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public void deleteEncRules(Collection<String> ruleNames) {
        if (ruleNames == null || ruleNames.isEmpty()) {
            return;
        }
        try (Connection conn = MetaDbUtil.getConnection()) {
            try {
                EncdbRuleAccessor accessor = new EncdbRuleAccessor();
                accessor.setConnection(conn);
                conn.setAutoCommit(false);
                for (String ruleName : ruleNames) {
                    accessor.deleteRuleByName(ruleName);
                }
                MetaDbConfigManager.getInstance().notify(MetaDbDataIdBuilder.ENCDB_RULE_DATA_ID, conn);
                conn.commit();
                // wait for all cn to load metadb
                MetaDbConfigManager.getInstance().sync(MetaDbDataIdBuilder.ENCDB_RULE_DATA_ID);
            } catch (SQLException e) {
                conn.rollback();
                throw GeneralUtil.nestedException(e);
            } finally {
                conn.setAutoCommit(true);
            }
        } catch (SQLException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public EncdbRule getEncRule(String ruleName) {
        return ruleMatchTree.getRule(ruleName);
    }

    public EncdbRuleMatchTree getRuleMatchTree() {
        return ruleMatchTree;
    }

    protected static class EncdbRuleConfigListener implements ConfigListener {
        @Override
        public void onHandleConfig(String dataId, long newOpVersion) {
            EncdbRuleManager.getInstance().reloadEncRules();
        }
    }

}
