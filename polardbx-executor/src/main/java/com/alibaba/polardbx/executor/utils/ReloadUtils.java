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

package com.alibaba.polardbx.executor.utils;

import com.alibaba.polardbx.atom.CacheVariables;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.group.utils.VariableProxy;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.statistic.inf.SystemTableTableStatistic;
import com.alibaba.polardbx.optimizer.view.SystemTableView;
import com.alibaba.polardbx.rule.TddlRule;

import javax.sql.DataSource;

/**
 * @author minggong 2017-09-14 15:49
 */
public class ReloadUtils {

    private static final Logger logger = LoggerFactory.getLogger(ReloadUtils.class);

    /**
     * 重建数据源
     */
    public static void reloadDataSources(ExecutorContext executorContext, OptimizerContext optimizerContext) {
        if (ConfigDataMode.isFastMock()) {
            return;
        }
        executorContext.getSequenceManager().destroy();
        optimizerContext.getRuleManager().destroy();
        executorContext.getSequenceManager().destroy();
        optimizerContext.getRuleManager().destroy();
        GsiMetaManager.invalidateCache(executorContext.getTopologyHandler().getAppName());
        SystemTableView.invalidateAll();

        TddlRule tddlRule = optimizerContext.getRuleManager().getTddlRule();

        executorContext.getTopologyHandler().reload();
        executorContext.getSequenceManager().init();

        optimizerContext.getRuleManager().init();
        try {
            DataSource ds = MetaDbDataSource.getInstance().getDataSource();
            optimizerContext.getViewManager().getSystemTableView().resetDataSource(ds);

            TGroupDataSource variableDs = (TGroupDataSource) executorContext.getTopologyHandler()
                .get(optimizerContext.getRuleManager().getTddlRule().getDefaultDbIndex())
                .getDataSource();
            ((VariableProxy) optimizerContext.getVariableManager().getVariableProxy()).resetDataSource(variableDs);
            optimizerContext.getVariableManager().invalidateAll();
            CacheVariables.invalidateAll();

        } catch (Exception e) {
            logger.error("reset dataSource fail", e);
        }

    }

    public enum ReloadType {
        USERS, SCHEMA, DATASOURCES, FILESTORAGE, PROCEDURES, FUNCTIONS, JAVA_FUNCTIONS, STATISTICS, COLUMNARMANAGER,
        COLUMNARMANAGER_CACHE, COLUMNARMANAGER_SNAPSHOT, COLUMNARMANAGER_SCHEMA
    }
}
