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

package com.alibaba.polardbx.optimizer;

import com.alibaba.polardbx.common.DefaultSchema;
import com.alibaba.polardbx.common.model.Group;
import com.alibaba.polardbx.common.model.Matrix;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.gms.topology.DbGroupInfoManager;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.optimizer.config.schema.DefaultDbSchema;
import com.alibaba.polardbx.optimizer.config.schema.InformationSchema;
import com.alibaba.polardbx.optimizer.config.schema.MetaDbSchema;
import com.alibaba.polardbx.optimizer.config.server.IServerConfigManager;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticManager;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.rule.Partitioner;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import com.alibaba.polardbx.optimizer.utils.OptimizerHelper;
import com.alibaba.polardbx.optimizer.variable.VariableManager;
import com.alibaba.polardbx.optimizer.view.ViewManager;
import com.alibaba.polardbx.stats.MatrixStatistics;
import com.alibaba.polardbx.stats.SchemaTransactionStatistics;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * 优化器上下文，主要解决一些共享上下文对象，因为不考虑spring进行IOC控制，所以一些对象/工具之间的依赖就很蛋疼，就搞了这么一个上下文
 * <p>
 * <pre>
 * 考虑基于ThreadLocal进行上下文传递，几个原因：
 * 1. 减少context传递，ast/expression/plan基本都是无状态的，不希望某个特定代码需要依赖context，而导致整个链路对象都需要传递context上下文
 * 2. context上下文中的对象，本身为支持线程安全，每个tddl客户端实例只有一份，一个jvm实例允许多个tddl客户端实例，所以不能搞成static对象
 * </pre>
 *
 * @author jianghang 2013-11-12 下午3:07:19
 */
public class OptimizerContext {

    // 配置信息
    private Matrix matrix;
    private SchemaManager schemaManager;
    private TddlRuleManager rule;
    private Partitioner partitioner;
    private String schemaName;
    private MatrixStatistics statistics;
    private ViewManager viewManager;
    private VariableManager variableManager;
    private PartitionInfoManager partitionInfoManager;
    private TableGroupInfoManager tableGroupInfoManager;
    private ParamManager paramManager;
    private boolean sqlMock = false;

    /**
     * <pre>
     *  KEY: APP_NAME
     *
     *  VAL: OptimizerContext
     * </pre>
     */
    private static Map<String, OptimizerContext> optimizerContextMap =
        new ConcurrentHashMap<String, OptimizerContext>();

    public static Set<String> getActiveSchemaNames() {
        return optimizerContextMap.keySet().stream()
            .filter(x -> !x.equalsIgnoreCase(DefaultDbSchema.NAME) &&
                !x.equalsIgnoreCase(SystemDbHelper.CDC_DB_NAME))
            .map(x -> getContext(x).getSchemaName())
            .collect(Collectors.toSet());
    }

    public static List<Group> getActiveGroups() {
        return optimizerContextMap.values().stream()
            .map(x -> getContext(x.getSchemaName()).getMatrix().getGroups())
            .flatMap(Collection::stream)
            .filter(DbGroupInfoManager::isVisibleGroup)
            .collect(Collectors.toList());
    }

    // load context once when TDatSource init
    public static void loadContext(OptimizerContext context) {
        optimizerContextMap.put(context.getSchemaName().toLowerCase(), context);
        DefaultSchema.setSchemaName(context.getSchemaName());
    }

    public static void setContext(OptimizerContext context) {
        DefaultSchema.setSchemaName(context.getSchemaName());
    }

    public static OptimizerContext getContext(String schemaName) {
        if (TStringUtil.isEmpty(schemaName)) {
            schemaName = DefaultSchema.getSchemaName();
        }
        if (schemaName == null) {
            return null;
        }
        if (schemaName.equalsIgnoreCase(MetaDbSchema.NAME)) {
            schemaName = InformationSchema.NAME;
        }
        String schemaNameLowerCase = schemaName.toLowerCase();
        OptimizerContext optimizerContext = optimizerContextMap.get(schemaNameLowerCase);
        if (optimizerContext == null) {
            IServerConfigManager serverConfigManager = OptimizerHelper.getServerConfigManager();
            if (serverConfigManager != null) {
                // When running unit test, ServerConfigManager could be null
                serverConfigManager.getAndInitDataSourceByDbName(schemaName);
                // In case of specified context has not been initialized.
            }
            optimizerContext = optimizerContextMap.get(schemaNameLowerCase);
        }

        return optimizerContext;
    }

    public static void clearContext(String schemaName) {
        optimizerContextMap.remove(schemaName.toLowerCase());
    }

    public OptimizerContext(String schemaName) {
        assert schemaName != null;
        this.schemaName = schemaName;
    }

    private OptimizerContext() {
        throw new AssertionError("DO NOT new a OptimizerContext without schemaName");
    }

    public Matrix getMatrix() {
        return matrix;
    }

    public void setMatrix(Matrix matrix) {
        this.matrix = matrix;
    }

    public SchemaManager getLatestSchemaManager() {
        return schemaManager;
    }

    public void setSchemaManager(SchemaManager schemaManager) {
        this.schemaManager = schemaManager;
    }

    public TddlRuleManager getRuleManager() {
        return rule;
    }

    public void setRuleManager(TddlRuleManager rule) {
        this.rule = rule;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public MatrixStatistics getStatistics() {
        return statistics;
    }

    public void setStatistics(MatrixStatistics statistics) {
        this.statistics = statistics;
    }

    public StatisticManager getStatisticManager() {
        return StatisticManager.getInstance();
    }

    public void setStatisticManager(StatisticManager statisticManager) {
    }

    public void setViewManager(ViewManager viewManager) {
        this.viewManager = viewManager;
    }

    public ViewManager getViewManager() {
        return viewManager;
    }

    public VariableManager getVariableManager() {
        return variableManager;
    }

    public void setVariableManager(VariableManager variableManager) {
        this.variableManager = variableManager;
    }

    public boolean isSqlMock() {
        return sqlMock;
    }

    public void setSqlMock(boolean sqlMock) {
        this.sqlMock = sqlMock;
    }

    public PartitionInfoManager getPartitionInfoManager() {
        return partitionInfoManager;
    }

    public void setPartitionInfoManager(PartitionInfoManager partitionInfoManager) {
        this.partitionInfoManager = partitionInfoManager;
    }

    public TableGroupInfoManager getTableGroupInfoManager() {
        return tableGroupInfoManager;
    }

    public void setTableGroupInfoManager(TableGroupInfoManager tableGroupInfoManager) {
        this.tableGroupInfoManager = tableGroupInfoManager;
    }

    public Partitioner getPartitioner() {
        return partitioner;
    }

    public void setPartitioner(Partitioner partitioner) {
        this.partitioner = partitioner;
    }

    public ParamManager getParamManager() {
        return paramManager;
    }

    public void setParamManager(ParamManager paramManager) {
        this.paramManager = paramManager;
    }

    public static SchemaTransactionStatistics getTransStat(String schema) {
        OptimizerContext context = getContext(schema);
        if (DynamicConfig.getInstance().isEnableTransactionStatistics() && null != context) {
            return context.statistics.getTransactionStats();
        }
        return null;
    }

}
