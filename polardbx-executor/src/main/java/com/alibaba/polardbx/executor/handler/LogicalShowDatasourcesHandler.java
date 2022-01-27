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

package com.alibaba.polardbx.executor.handler;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.polardbx.atom.TAtomDataSource;
import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.model.Group;
import com.alibaba.polardbx.common.model.Matrix;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.common.TopologyHandler;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.spi.IGroupExecutor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.ha.impl.StorageHaManager;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.alibaba.polardbx.group.config.Weight;
import com.alibaba.polardbx.group.jdbc.DataSourceWrapper;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.rpc.XConfig;
import com.alibaba.polardbx.rpc.compatible.XDataSource;
import com.alibaba.polardbx.rpc.pool.XClientPool;
import com.alibaba.polardbx.rpc.pool.XConnectionManager;
import org.apache.calcite.rel.RelNode;

import javax.sql.DataSource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author chenmo.cm
 */
public class LogicalShowDatasourcesHandler extends HandlerCommon {

    private static final Logger logger = LoggerFactory.getLogger(LogicalShowDatasourcesHandler.class);

    public LogicalShowDatasourcesHandler(IRepository repo) {
        super(repo);
    }

    public TAtomDataSource getAtomDatasource(DataSource s) {
        if (s instanceof TAtomDataSource) {
            return (TAtomDataSource) s;
        }

        if (s instanceof DataSourceWrapper) {
            return getAtomDatasource(((DataSourceWrapper) s).getWrappedDataSource());
        }

        throw new IllegalAccessError();
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        ArrayResultCursor result = new ArrayResultCursor("DATASOURCES");
        result.addColumn("ID", DataTypes.IntegerType);
        result.addColumn("SCHEMA", DataTypes.StringType);
        result.addColumn("NAME", DataTypes.StringType);

        // 虽然GROUP是关键字，可以用反引用绕过
        result.addColumn("GROUP", DataTypes.StringType);
        result.addColumn("URL", DataTypes.StringType);
        result.addColumn("USER", DataTypes.StringType);
        result.addColumn("TYPE", DataTypes.StringType);
        result.addColumn("INIT", DataTypes.StringType);
        result.addColumn("MIN", DataTypes.StringType);

        result.addColumn("MAX", DataTypes.StringType);
        result.addColumn("IDLE_TIMEOUT", DataTypes.StringType);
        result.addColumn("MAX_WAIT", DataTypes.StringType);
        result.addColumn("ON_FATAL_ERROR_MAX_ACTIVE", DataTypes.StringType);
        result.addColumn("MAX_WAIT_THREAD_COUNT", DataTypes.StringType);

        result.addColumn("ACTIVE_COUNT", DataTypes.StringType);
        result.addColumn("POOLING_COUNT", DataTypes.StringType);

        result.addColumn("ATOM", DataTypes.StringType);
        result.addColumn("READ_WEIGHT", DataTypes.StringType);
        result.addColumn("WRITE_WEIGHT", DataTypes.StringType);

        result.addColumn("STORAGE_INST_ID", DataTypes.StringType);
        result.initMeta();
        Integer index = 0;
        Matrix matrix = ExecutorContext.getContext(executionContext.getSchemaName()).getTopologyHandler().getMatrix();
        TopologyHandler topology = ExecutorContext.getContext(executionContext.getSchemaName()).getTopologyHandler();
        List<Group> groups = matrix.getGroups();
        fillDataSourceInfos(result, topology, index, groups);
        List<Group> scaleGroups = matrix.getScaleOutGroups();
        fillDataSourceInfos(result, topology, index, scaleGroups);
        fillMetaDbDataSourceInfo(result, index);

        return result;
    }

    private void fillMetaDbDataSourceInfo(ArrayResultCursor result, Integer index) {
        DataSource dataSource = MetaDbDataSource.getInstance().getDataSource();
        if (dataSource instanceof MetaDbDataSource.MetaDbDataSourceHaWrapper) {
            MetaDbDataSource.MetaDbDataSourceHaWrapper dsHaWrapper =
                (MetaDbDataSource.MetaDbDataSourceHaWrapper) dataSource;
            final DataSource phyDataSource = dsHaWrapper.getRawDataSource();
            String appName = "None";
            String grpName = "MetaDB";
            String dbKey = "MetaDbKey";
            String storageInstId = StorageHaManager.getInstance().getMetaDbStorageInstId();
            if (phyDataSource instanceof XDataSource) {
                final XConnectionManager m = XConnectionManager.getInstance();
                final XDataSource x = (XDataSource) phyDataSource;
                final XClientPool.XStatus s = x.getStatus();
                result.addRow(new Object[] {
                    index++, appName, x.getName(), grpName,
                    x.getUrl(), XConnectionManager.getInstance().isEnableAuth() ? x.getUsername() : XConfig.X_USER,
                    XConfig.X_TYPE, 0,
                    m.getMinPooledSessionPerInstance(),
                    m.getMaxClientPerInstance() * m.getMaxSessionPerClient(),
                    MetaDbDataSource.getInstance().getConf().getIdleTimeout(), x.getGetConnTimeoutMillis(),
                    DynamicConfig.getInstance().getXprotoMaxDnWaitConnection(),
                    DynamicConfig.getInstance().getXprotoMaxDnConcurrent(), s.workingSession, s.idleSession, dbKey,
                    "10", "10", storageInstId});
            } else {
                throw new NotSupportException("jdbc not support");
            }
        }
    }

    private void fillDataSourceInfos(ArrayResultCursor result, TopologyHandler topology,
                                     Integer index, List<Group> groups) {

        for (Group group : groups) {
            IGroupExecutor groupExecutor = topology.get(group.getName());
            if (groupExecutor == null) {
                continue;
            }

            Object o = groupExecutor.getDataSource();

            if (o instanceof TGroupDataSource) {
                TGroupDataSource ds = (TGroupDataSource) o;

                // 整理atom的权重信息
                Map<TAtomDataSource, Weight> atomDsWeights = ds.getAtomDataSourceWeights();
                Map<String, Weight> atomKeyWeightMaps = new HashMap<String, Weight>();
                for (Map.Entry<TAtomDataSource, Weight> atomWeight : atomDsWeights.entrySet()) {
                    TAtomDataSource atomDs = atomWeight.getKey();
                    Weight weight = atomWeight.getValue();
                    atomKeyWeightMaps.put(atomDs.getDbKey(), weight);
                }

                for (DataSource atom : ds.getAtomDataSources()) {
                    TAtomDataSource atomDs = this.getAtomDatasource(atom);
                    String dbKey = atomDs.getDbKey();
                    Weight weightVal = atomKeyWeightMaps.get(dbKey);
                    int w = -1;
                    int r = -1;
                    if (weightVal != null) {
                        w = weightVal.w;
                        r = weightVal.r;
                    }

                    final DataSource rawDataSource = atomDs.getDataSource();
                    if (rawDataSource instanceof XDataSource) {
                        final XConnectionManager m = XConnectionManager.getInstance();
                        final XDataSource x = (XDataSource) rawDataSource;
                        final XClientPool.XStatus s = x.getStatus();

                        String storageInstId = GroupInfoUtil.parseStorageId(dbKey);
                        result.addRow(new Object[] {
                            index++, topology.getAppName(), x.getName(), group.getName(),
                            x.getUrl(),
                            XConnectionManager.getInstance().isEnableAuth() ? x.getUsername() : XConfig.X_USER,
                            XConfig.X_TYPE, 0,
                            m.getMinPooledSessionPerInstance(),
                            m.getMaxClientPerInstance() * m.getMaxSessionPerClient(),
                            atomDs.getDsConfHandle().getRunTimeConf().getIdleTimeout(), x.getGetConnTimeoutMillis(),
                            DynamicConfig.getInstance().getXprotoMaxDnWaitConnection(),
                            DynamicConfig.getInstance().getXprotoMaxDnConcurrent(), s.workingSession, s.idleSession,
                            dbKey, r, w, storageInstId});
                    } else if (rawDataSource instanceof DruidDataSource) {
                        DruidDataSource d = (DruidDataSource) rawDataSource;

                        String storageInstId = GroupInfoUtil.parseStorageId(dbKey);
                        result.addRow(new Object[] {
                            index++, topology.getAppName(), d.getName(), group.getName(),
                            d.getUrl(), d.getUsername(), d.getDbType(), d.getInitialSize(), d.getMinIdle(),
                            d.getMaxActive(), atomDs.getDsConfHandle().getRunTimeConf().getIdleTimeout(),
                            d.getMaxWait(),
                            d.getOnFatalErrorMaxActive(), d.getMaxWaitThreadCount(), d.getActiveCount(),
                            d.getPoolingCount(), dbKey, r, w,
                            storageInstId});
                    } else {
                        throw new NotSupportException("jdbc not support");
                    }
                } // end of for
            } // end of if
        } // end of for
    }
}
