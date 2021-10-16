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

import com.alibaba.polardbx.atom.TAtomDataSource;
import com.alibaba.polardbx.atom.config.TAtomDsConfDO;
import com.alibaba.polardbx.common.TddlNode;
import com.alibaba.polardbx.common.constants.SequenceAttribute;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.jdbc.MasterSlave;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.model.RepoInst;
import com.alibaba.polardbx.common.model.SqlType;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.MetricLevel;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.properties.PropUtil;
import com.alibaba.polardbx.common.utils.ExecutorMode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.optimizer.chunk.Chunk;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.common.TopologyHandler;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.mpp.deploy.ServiceProvider;
import com.alibaba.polardbx.executor.mpp.discover.RefreshNodeSyncAction;
import com.alibaba.polardbx.executor.mpp.execution.QueryInfo;
import com.alibaba.polardbx.executor.mpp.execution.StageInfo;
import com.alibaba.polardbx.executor.mpp.execution.TaskInfo;
import com.alibaba.polardbx.executor.operator.util.ConcurrentRawHashTable;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.node.GmsNodeManager;
import com.alibaba.polardbx.gms.node.InternalNode;
import com.alibaba.polardbx.gms.node.InternalNodeManager;
import com.alibaba.polardbx.gms.node.Node;
import com.alibaba.polardbx.gms.node.NodeStatusManager;
import com.alibaba.polardbx.gms.sync.IGmsSyncAction;
import com.alibaba.polardbx.group.jdbc.DataSourceWrapper;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.expression.bean.NullValue;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.BaseQueryOperation;
import com.alibaba.polardbx.optimizer.core.rel.BaseTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.BroadcastTableModify;
import com.alibaba.polardbx.optimizer.core.rel.DirectTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.HashGroupJoin;
import com.alibaba.polardbx.optimizer.core.rel.HashJoin;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
import com.alibaba.polardbx.optimizer.core.rel.LogicalModify;
import com.alibaba.polardbx.optimizer.core.rel.LogicalModifyView;
import com.alibaba.polardbx.optimizer.core.rel.LogicalRelocate;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.SingleTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.UnionOptHelper;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import com.alibaba.polardbx.optimizer.core.rel.dal.PhyShow;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.utils.IDistributedTransaction;
import com.alibaba.polardbx.optimizer.utils.QueryConcurrencyPolicy;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.repo.mysql.spi.MyDataSourceGetter;
import com.alibaba.polardbx.sequence.Sequence;
import com.alibaba.polardbx.sequence.exception.SequenceException;
import com.alibaba.polardbx.sequence.impl.BaseSequence;
import com.alibaba.polardbx.util.IntBloomFilter;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import it.unimi.dsi.fastutil.HashCommon;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.sql.SqlSelect;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static com.alibaba.polardbx.common.jdbc.ITransactionPolicy.TransactionClass.EXPLICIT_TRANSACTION;
import static com.alibaba.polardbx.common.jdbc.ITransactionPolicy.TransactionClass.SUPPORT_SHARE_READVIEW_TRANSACTION;
import static com.alibaba.polardbx.common.properties.ConnectionParams.SUPPORT_READ_FOLLOWER_STRATEGY;
import static com.alibaba.polardbx.common.utils.thread.ThreadCpuStatUtil.NUM_CORES;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.MoreFutures.tryGetFutureValue;

public class ExecUtils {

    private static final Logger logger = LoggerFactory.getLogger(ExecUtils.class);

    private static final int MAX_PARALLELISM = 16;

    public static long calcIo(LogicalView logicalView) {
        RelMetadataQuery mq = logicalView.getCluster().getMetadataQuery();
        synchronized (mq) {
            return (long) mq.getCumulativeCost(logicalView).getIo();
        }
    }

    public static long calcRowCount(LogicalView logicalView) {
        RelMetadataQuery mq = logicalView.getCluster().getMetadataQuery();
        synchronized (mq) {
            return mq.getRowCount(logicalView).longValue();
        }
    }

    public static int getMppMaxParallelism(ParamManager paramManager, boolean master) {
        int maxParallelism = paramManager.getInt(ConnectionParams.MPP_MAX_PARALLELISM);
        if (maxParallelism < 1) {
            int num = (ServiceProvider.getInstance().getServer()).getNodeManager()
                .getAllWorkers(ConfigDataMode.isMasterMode() && paramManager
                    .getBoolean(ConnectionParams.POLARDBX_SLAVE_INSTANCE_FIRST)).size();
            int cores = getPolarDBXCores(paramManager, master);
            maxParallelism = num * cores;
        }
        return maxParallelism;
    }

    public static int getMppMinParallelism(ParamManager paramManager) {
        int minParallelism = paramManager
            .getInt(ConnectionParams.MPP_MIN_PARALLELISM);
        if (minParallelism < 1) {
            return 4;
        }
        return minParallelism;
    }

    /**
     * 获取polarX Server节点的cpu核数
     */
    public static int getPolarDBXCores(ParamManager paramManager, boolean master) {
        int polarXParallelism = paramManager.getInt(ConnectionParams.POLARDBX_PARALLELISM);
        if (polarXParallelism < 1) {
            if (master) {
                if (ConfigDataMode.isMasterMode()) {
                    polarXParallelism = NUM_CORES;
                } else {
                    GmsNodeManager gmsNodeManager = GmsNodeManager.getInstance();
                    if (gmsNodeManager.getReadOnlyNodes().size() > 0) {
                        polarXParallelism =
                            gmsNodeManager.getReadOnlyNodeCpuCore() > 0 ? gmsNodeManager.getReadOnlyNodeCpuCore() :
                                NUM_CORES;
                    } else {
                        polarXParallelism = NUM_CORES;
                    }
                }
            } else {
                if (!ConfigDataMode.isMasterMode()) {
                    polarXParallelism = NUM_CORES;
                } else {
                    GmsNodeManager gmsNodeManager = GmsNodeManager.getInstance();
                    if (gmsNodeManager.getReadOnlyNodes().size() > 0) {
                        polarXParallelism =
                            gmsNodeManager.getReadOnlyNodeCpuCore() > 0 ? gmsNodeManager.getReadOnlyNodeCpuCore() :
                                NUM_CORES;
                    } else {
                        polarXParallelism = NUM_CORES;
                    }
                }
            }
        }
        return polarXParallelism;
    }

    /**
     * 获取polarX实例下挂载的mysql的cpu核数
     */
    public static int getPolarDbCores(ParamManager paramManager, boolean master) {
        int dbParallelism = paramManager.getInt(ConnectionParams.DATABASE_PARALLELISM);
        if (dbParallelism < 1) {
            //TODO 现在存储节点和计算节点的CPU核数一致，以后待定
            dbParallelism = getPolarDBXCores(paramManager, master);
        }
        return dbParallelism;
    }

    public static int getMppPrefetchNumForLogicalView(int num) {
        //TODO 需要结合PolarDB的个数和核数来决定默认Prefetch数量
        return Math.min(num, NUM_CORES * 4);
    }

    public static int getPrefetchNumForLogicalView(int num) {
        return Math.min(num, NUM_CORES * 4);
    }

    public static int getParallelismForLogicalView(int shardCount) {
        return Math.min(Math.min(shardCount, NUM_CORES), MAX_PARALLELISM);
    }

    public static int getParallelismForLocal(ExecutionContext context) {
        int parallelism = context.getParamManager().getInt(ConnectionParams.PARALLELISM);
        if (parallelism < 0) {
            parallelism = Math.min(NUM_CORES, MAX_PARALLELISM);
        } else if (parallelism == 0) {
            parallelism = 1;
        }
        return parallelism;
    }

    public static List<Map<String, Object>> resultSetToList(ResultCursor rs) {

        if (rs == null) {
            return null;
        }

        List<Map<String, Object>> results = new ArrayList<>();

        try {

            Row row;
            while ((row = rs.next()) != null) {
                Map<String, Object> rowMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

                List<ColumnMeta> columns = row.getParentCursorMeta().getColumns();
                for (int i = 0; i < columns.size(); i++) {
                    rowMap.put(columns.get(i).getName(), row.getObject(i));
                }

                results.add(rowMap);
            }
        } catch (Exception ex) {
            throw GeneralUtil.nestedException(ex);
        }

        return results;
    }

    public static boolean allElementsNull(List args) {
        boolean allArgsNull = true;
        for (Object arg : args) {
            allArgsNull = allArgsNull && ExecUtils.isNull(arg);
        }
        return allArgsNull;
    }

    public static List<Map<String, Object>> resultSetToList(ResultSet rs) {

        if (rs == null) {
            return null;
        }

        List<Map<String, Object>> results = new ArrayList<>();

        try {
            ResultSetMetaData meta = rs.getMetaData();
            while (rs.next()) {
                Map<String, Object> row = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

                for (int i = 1; i <= meta.getColumnCount(); i++) {
                    row.put(meta.getColumnName(i), rs.getObject(i));
                }

                results.add(row);
            }
        } catch (Exception ex) {
            throw GeneralUtil.nestedException(ex);
        }

        return results;
    }

    /**
     * 根据order by 条件，从left和right KVPair里面拿到一个列所对应的值(从key或者从value里面） 然后进行比较。
     * 相等则继续比较其他。 不相等则根据asc desc决定大小。
     */
    public static Comparator<Row> getComparator(final List<OrderByOption> orderBys, List<DataType> columnMetas) {
        Preconditions.checkArgument(columnMetas != null);
        return new Comparator<Row>() {

            @Override
            public int compare(Row o1, Row o2) {

                for (OrderByOption option : orderBys) {
                    Object c1 = o1.getObjectForCmp(option.index);
                    Object c2 = o2.getObjectForCmp(option.index);

                    if (c1 == null && c2 == null) {
                        continue;
                    }
                    int n = comp(c1, c2, columnMetas.get(option.index), option.asc);
                    if (n == 0) {
                        continue;
                    }
                    return n;
                }
                return 0;
            }
        };

    }

    public static int comp(Object c1, Object c2, DataType type, boolean isAsc) {
        if (type == null) {
            type = DataTypeUtil.getTypeOfObject(c1);
        }
        int n = type.compare(c1, c2);
        if (n == 0) {
            return n;
        }
        if (isAsc) {
            return n;
        } else {
            return n < 0 ? 1 : -1;
        }
    }

    public static boolean isNull(Object o) {
        if (o instanceof com.alibaba.polardbx.optimizer.core.function.calc.scalar.filter.Row.RowValue) {
            return ExecUtils.allElementsNull(
                ((com.alibaba.polardbx.optimizer.core.function.calc.scalar.filter.Row.RowValue) o).getValues());
        }
        if (o == null) {
            return true;
        }
        if (o instanceof NullValue) {
            return true;
        }
        return false;
    }

    public static void closeStatement(java.sql.Statement stmt) {
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException sqlEx) {
                // ignore
            }
        }
    }

    public static MasterSlave getMasterSlave(boolean inTrans, boolean isWrite, ExecutionContext ec) {
        MasterSlave masterSlaveVal = MasterSlave.READ_WEIGHT;
        if (isWrite) {
            masterSlaveVal = MasterSlave.MASTER_ONLY;
        } else if (inTrans) {
            if (ConfigDataMode.enableSlaveReadForPolarDbX() && ec.isAutoCommit()) {
                //autocommit为true的时候才可能走备库
                return getFollowerStrategy(ec);
            } else {
                masterSlaveVal = MasterSlave.MASTER_ONLY;
            }
        } else if (ec != null && ec.getExtraCmds() != null) {
            if (ec.getExtraCmds().containsKey(ConnectionProperties.MASTER)) {
                masterSlaveVal = MasterSlave.MASTER_ONLY;
            } else if (ec.getExtraCmds().containsKey(ConnectionProperties.SLAVE)) {
                masterSlaveVal =
                    ConfigDataMode.enableSlaveReadForPolarDbX() ? getFollowerStrategy(ec) : MasterSlave.MASTER_ONLY;
            } else if (ConfigDataMode.enableSlaveReadForPolarDbX()) {
                masterSlaveVal = getFollowerStrategy(ec);
            }
        } else if (ConfigDataMode.enableSlaveReadForPolarDbX()) {
            masterSlaveVal = getFollowerStrategy(ec);
        }
        return masterSlaveVal;
    }

    private static MasterSlave getFollowerStrategy(ExecutionContext ec) {
        String stategy = ec.getParamManager().getString(SUPPORT_READ_FOLLOWER_STRATEGY);
        if (stategy.equalsIgnoreCase(PropUtil.FOLLOWERSTRATEGY.FORCE.toString())) {
            return MasterSlave.SLAVE_ONLY;
        } else if (stategy.equalsIgnoreCase(PropUtil.FOLLOWERSTRATEGY.AUTO.toString())) {
            if (ExecUtils.isTpMode(ec)) {
                return MasterSlave.MASTER_ONLY;
            } else {
                return MasterSlave.SLAVE_ONLY;
            }
        } else {
            return MasterSlave.MASTER_ONLY;
        }
    }

    public static QueryConcurrencyPolicy getQueryConcurrencyPolicy(ExecutionContext executionContext) {
        return getQueryConcurrencyPolicy(executionContext, null);
    }

    public static QueryConcurrencyPolicy getQueryConcurrencyPolicy(ExecutionContext executionContext,
                                                                   LogicalView logicalView) {
        // if MERGE_UNION = false, force use SEQUENTIAL
        if (!executionContext.getParamManager().getBoolean(ConnectionParams.MERGE_UNION)) {
            return QueryConcurrencyPolicy.SEQUENTIAL;
        }

        if (executionContext.getParamManager().getBoolean(ConnectionParams.SEQUENTIAL_CONCURRENT_POLICY)) {
            return QueryConcurrencyPolicy.SEQUENTIAL;
        }

        // for broadcast table write
        if (executionContext.getParamManager().getBoolean(ConnectionParams.FIRST_THEN_CONCURRENT_POLICY)) {
            return QueryConcurrencyPolicy.FIRST_THEN_CONCURRENT;
        }

        // Force SEQUENTIAL to reduce deadlocks in transactions, unless for SELECT statements
        if (executionContext.getTransaction() instanceof IDistributedTransaction
            && executionContext.getSqlType() != SqlType.SELECT && !executionContext.getParamManager().getBoolean(
            ConnectionParams.GSI_CONCURRENT_WRITE) && logicalView == null) {
            return QueryConcurrencyPolicy.SEQUENTIAL;
        }

        if (executionContext.getParamManager().getBoolean(ConnectionParams.GROUP_CONCURRENT_BLOCK)) {
            if (logicalView != null && (logicalView.isUnderMergeSort() || executionContext.getParamManager()
                .getBoolean(ConnectionParams.MERGE_CONCURRENT))) {
                return QueryConcurrencyPolicy.CONCURRENT;
            }
            return QueryConcurrencyPolicy.GROUP_CONCURRENT_BLOCK;
        }

        if (executionContext.getParamManager().getBoolean(ConnectionParams.BLOCK_CONCURRENT)) {
            return QueryConcurrencyPolicy.CONCURRENT;
        }

        return QueryConcurrencyPolicy.SEQUENTIAL;
    }

    private static int getMaxConnCountForSingleGroup(String schemaName, String groupName, ExecutionContext context,
                                                     LogicalView logicalView) {
        TGroupDataSource ds = new MyDataSourceGetter(schemaName).getDataSource(groupName);
        final Map<String, DataSourceWrapper> dataSourceWrapperMap = ds.getConfigManager()
            .getDataSourceWrapperMap();
        Entry<String, DataSourceWrapper> atomEntry = dataSourceWrapperMap.entrySet().iterator().next();
        if (atomEntry == null) {
            return 1;
        } else {
            DataSourceWrapper dataSourceWrapper = atomEntry.getValue();
            TAtomDataSource atom = dataSourceWrapper.getWrappedDataSource();
            TAtomDsConfDO atomConfig = atom.getDsConfHandle().getRunTimeConf();
            // In insert select, we are inserting while selecting, which
            // costs double connections.
            double factor = context.isModifySelect() ? 0.3 : 0.6;
            return Math.min((int) Math.ceil(atomConfig.getMaxPoolSize() * factor), 10);
        }
    }

    public static int getAffectRowsByCursors(List<Cursor> cursors, boolean isBroadcast) {
        int affectRows = 0;
        try {
            for (int i = 0; i < cursors.size(); i++) {
                int rows = 0;
                Cursor inputCursor = cursors.get(i);

                // in batch mode, there are more than one RowSet.
                Row rs;
                while ((rs = inputCursor.next()) != null) {
                    rows += rs.getInteger(0);
                }

                if (isBroadcast) {
                    affectRows = rows;
                } else {
                    affectRows += rows;
                }

                inputCursor.close(new ArrayList<>());
                cursors.set(i, null);
            }
        } finally {
            for (Cursor inputCursor : cursors) {
                if (inputCursor != null) {
                    inputCursor.close(new ArrayList<>());
                }
            }
        }

        return affectRows;
    }

    public static int getAffectRowsByCursor(Cursor inputCursor) {
        int affectRows = 0;
        try {
            Row rs;
            while ((rs = inputCursor.next()) != null) {
                affectRows += rs.getInteger(0);
            }

            inputCursor.close(new ArrayList<>());
        } finally {
            inputCursor.close(new ArrayList<>());
        }

        return affectRows;
    }

    public static String getTargetDbGruop(RelNode relNode, ExecutionContext executionContext) {
        String group;
        if (relNode instanceof SingleTableOperation) {
            String schemaName = ((SingleTableOperation) relNode).getSchemaName();
            group = OptimizerContext.getContext(schemaName).getRuleManager().getDefaultDbIndex(null);
        } else if (relNode instanceof BaseTableOperation) {
            group = ((BaseTableOperation) relNode).getDbIndex();
        } else if (relNode instanceof LogicalInsert) {
            group = OptimizerContext.getContext(((LogicalInsert) relNode).getSchemaName())
                .getRuleManager()
                .getDefaultDbIndex(null);
        } else if (relNode instanceof LogicalModify) {
            group = OptimizerContext.getContext(((LogicalModify) relNode).getSchemaName())
                .getRuleManager()
                .getDefaultDbIndex(null);
        } else if (relNode instanceof LogicalRelocate) {
            group = OptimizerContext.getContext(((LogicalRelocate) relNode).getSchemaName())
                .getRuleManager()
                .getDefaultDbIndex(null);
        } else if (relNode instanceof LogicalModifyView) {
            group = OptimizerContext.getContext(((LogicalModifyView) relNode).getSchemaName())
                .getRuleManager()
                .getDefaultDbIndex(null);
        } else if (relNode instanceof LogicalView) {
            group = OptimizerContext.getContext(((LogicalView) relNode).getSchemaName())
                .getRuleManager()
                .getDefaultDbIndex(null);
        } else if (relNode instanceof BroadcastTableModify) {
            group = OptimizerContext.getContext(((BroadcastTableModify) relNode).getDirectTableOperation()
                .getSchemaName())
                .getRuleManager()
                .getDefaultDbIndex(null);
        } else if (relNode instanceof LogicalShow) {
            group = ((LogicalShow) relNode).getDbIndex();
        } else if (relNode instanceof PhyShow) {
            String schemaName = ((PhyShow) relNode).getSchemaName();
            group = ((PhyShow) relNode).getDbIndex();
            if (group == null) {
                group = OptimizerContext.getContext(schemaName).getRuleManager().getDefaultDbIndex(null);
            }
        } else if (relNode instanceof BaseDdlOperation) {
            group = OptimizerContext.getContext(((BaseDdlOperation) relNode).getSchemaName())
                .getRuleManager()
                .getDefaultDbIndex(null);
        } else {
            group =
                OptimizerContext.getContext(executionContext.getSchemaName()).getRuleManager().getDefaultDbIndex(null);
        }
        return group;
    }

    public static boolean arrayEquals(byte[] arr1, int pos1, int len1, byte[] arr2, int pos2, int len2) {
        if (len1 != len2) {
            return false;
        }
        for (int i = 0; i < len1; i++) {
            if (arr1[pos1 + i] != arr2[pos2 + i]) {
                return false;
            }
        }
        return true;
    }

    public static boolean arrayEquals(
        char[] arr1, int pos1, int len1, char[] arr2, int pos2, int len2, boolean ingoreCase) {
        if (len1 != len2) {
            return false;
        }
        for (int i = 0; i < len1; i++) {
            if (ingoreCase) {
                if (Character.toUpperCase(arr1[pos1 + i]) != Character.toUpperCase(arr2[pos2 + i])) {
                    return false;
                }
            } else {
                if (arr1[pos1 + i] != arr2[pos2 + i]) {
                    return false;
                }
            }
        }
        return true;
    }

    public static int getTupleCount(LogicalValues logicalValues, ExecutionContext executionContext) {
        List tuples = logicalValues.getTuples();
        int count = tuples.size();
        if (logicalValues.getDynamicIndex() != null) {
            int index = logicalValues.getDynamicIndex() + 1;
            Object param = executionContext.getParams().getCurrentParameter().get(index).getValue();
            try {
                count = Integer.parseInt(param.toString());
            } catch (NumberFormatException e) {
                throw new TddlRuntimeException(ErrorCode.ERR_VALIDATE, "error sequence count value " + e);
            }
        }
        return count;
    }

    public static List<RelNode> getInputs(
        LogicalView logicalPlan, ExecutionContext executionContext, boolean forceIgnoreRF) {
        return logicalPlan.getInput(
            getUnionOptHelper(logicalPlan, executionContext), executionContext, forceIgnoreRF);
    }

    public static UnionOptHelper getUnionOptHelper(LogicalView logicalPlan, ExecutionContext executionContext) {
        return new UnionOptHelper() {
            @Override
            public int calMergeUnionSize(int total, String groupName) {
                // mock mode force union size=1
                if (ConfigDataMode.isFastMock()) {
                    return 1;
                }
                // Only one sql, do not need to union.
                if (total == 1) {
                    return 0;
                }

                // Compatible with DRDS 5.2
                if (!executionContext.getParamManager().getBoolean(ConnectionParams.MERGE_UNION)) {
                    return 1;
                }

                // When sql is in trx or going to take lock, only use one concurrent each group
                if (!allowMultipleReadConns(executionContext, logicalPlan)) {
                    // Only one connection, return 0 means union all.
                    return 0;
                }

                // Get mergeUnionSize from hint or connection properties.
                int mergeUnionSize =
                    executionContext.getParamManager().getInt(ConnectionParams.MERGE_UNION_SIZE);
                if (mergeUnionSize >= 0) {
                    return mergeUnionSize;
                }

                String schemaName = logicalPlan.getSchemaName();
                if (StringUtils.isEmpty(schemaName)) {
                    schemaName = executionContext.getSchemaName();
                }
                // Use default size, MIN(10, ceil(maxPoolSize * 0.6))
                int maxConnCount = getMaxConnCountForSingleGroup(schemaName, groupName,
                    executionContext, logicalPlan);
                if (maxConnCount == 1) {
                    // Only one connection, return 0 means union all.
                    return 0;
                } else {
                    // The min value is 1, means do not use union optimizer.
                    return (int) Math.ceil((double) total / maxConnCount);
                }
            }
        };
    }

    public static boolean useExplicitTransaction(ExecutionContext context) {
        //Autocommit is true, but the GSI must be in transaction.
        boolean ret = context.getTransaction().getTransactionClass().isA(EXPLICIT_TRANSACTION);
        return ret && ConfigDataMode.isMasterMode() && !ExecUtils.isMppMode(context);
    }

    public static boolean allowMultipleReadConns(ExecutionContext context, LogicalView logicalView) {
        boolean ret = useExplicitTransaction(context);
        if (ret) {
            boolean shareReadView = context.isShareReadView() && context.getTransaction().
                getTransactionClass().isA(SUPPORT_SHARE_READVIEW_TRANSACTION);
            if (!shareReadView && !context.isAutoCommit()) {
                return false;
            } else {
                if (context.getSqlType() != SqlType.SELECT) {
                    return false;
                }
                if (logicalView != null) {
                    return ((IDistributedTransaction) context.getTransaction()).allowMultipleReadConns()
                        && logicalView.getLockMode() == SqlSelect.LockMode.UNDEF;
                } else {
                    return ((IDistributedTransaction) context.getTransaction()).allowMultipleReadConns();
                }
            }
        } else {
            return true;
        }
    }

    public static String buildDRDSTraceComment(ExecutionContext context) {
        StringBuilder append = new StringBuilder();
        Object clientIp = context.getClientIp();
        Object traceId = context.getTraceId();
        Object server_id = "";
        Object phySqlId = context.getPhySqlId();
        final Map<String, Object> extraVariables = context.getExtraServerVariables();
        if (null != extraVariables && extraVariables.containsKey("polardbx_server_id")) {
            server_id = extraVariables.get("polardbx_server_id");
        }
        append.append("/*DRDS /").append(clientIp).append("/").append(traceId).append("/");
        append.append(phySqlId).append("/").append(server_id).append("/ */");
        return append.toString();
    }

    public static Sequence mockSeq(String name) {
        BaseSequence sequence = new BaseSequence() {
            long seq = 0;

            @Override
            public long nextValue() throws SequenceException {
                return seq++;
            }

            @Override
            public long nextValue(int size) throws SequenceException {
                return seq + size;
            }

            @Override
            public boolean exhaustValue() throws SequenceException {
                return false;
            }
        };
        sequence.setName(name);
        sequence.setType(SequenceAttribute.Type.GROUP);
        return sequence;
    }

    public static void buildOneChunk(Chunk keyChunk, int position, ConcurrentRawHashTable hashTable,
                                     int[] positionLinks,
                                     IntBloomFilter bloomFilter) {
        // Calculate hash codes of the whole chunk
        int[] hashes = keyChunk.hashCodeVector();

        if (checkJoinKeysAllNotNull(keyChunk)) {
            // If all keys are not null, we can leave out the null-check procedure
            for (int offset = 0; offset < keyChunk.getPositionCount(); offset++, position++) {
                int next = hashTable.put(position, hashes[offset]);
                if (next != -1) {
                    int a = 1;
                }
                positionLinks[position] = next;
                if (bloomFilter != null) {
                    bloomFilter.put(hashes[offset]);
                }
            }
        } else {
            // Otherwise we have to check nullability for each row
            for (int offset = 0; offset < keyChunk.getPositionCount(); offset++, position++) {
                if (checkJoinKeysNotNull(keyChunk, offset)) {
                    int next = hashTable.put(position, hashes[offset]);
                    positionLinks[position] = next;
                    if (bloomFilter != null) {
                        bloomFilter.put(hashes[offset]);
                    }
                }
            }
        }
    }

    public static boolean checkJoinKeysAllNotNull(Chunk keyChunk) {
        for (int i = 0; i < keyChunk.getBlockCount(); i++) {
            if (keyChunk.getBlock(i).mayHaveNull()) {
                return false;
            }
        }
        return true;
    }

    public static boolean checkJoinKeysNotNull(Chunk keyChunk, int offset) {
        for (int i = 0; i < keyChunk.getBlockCount(); i++) {
            if (keyChunk.getBlock(i).isNull(offset)) {
                return false;
            }
        }
        return true;
    }

    /**
     * 将inputList按RDS实例重排一下
     */
    public static List<RelNode> zigzagInputsByMysqlInst(List<RelNode> inputs, String schemaName) {

        List<RelNode> newInputs = new ArrayList<RelNode>(inputs.size());

        if (inputs.isEmpty()) {
            return newInputs;
        }

        BaseQueryOperation phyOperation = (BaseQueryOperation) inputs.get(0);

        Map<String, RepoInst> groupRepoInstMap =
            ExecutorContext.getContext(schemaName).getTopologyHandler().getGroupRepoInstMaps();

        int instCount = 0;
        List<List<RelNode>> instPhyRelArrList = new ArrayList<List<RelNode>>();
        Map<String, Integer> instIndexMap = new HashMap<String, Integer>();

        int maxPhyRelIndexOfOneInst = 0;
        for (int i = 0; i < inputs.size(); i++) {
            phyOperation = (BaseQueryOperation) inputs.get(i);
            String groupIndex = phyOperation.getDbIndex();
            RepoInst repoInst = groupRepoInstMap.get(groupIndex);
            String instAddr = repoInst.getAddress();
            Integer instIndex = instIndexMap.get(instAddr);

            List<RelNode> phyRelListOfInst = null;
            if (instIndex == null) {
                ++instCount;
                instIndex = instCount - 1;
                instIndexMap.put(instAddr, instIndex);
                phyRelListOfInst = new ArrayList<RelNode>();
                instPhyRelArrList.add(phyRelListOfInst);
            }
            phyRelListOfInst = instPhyRelArrList.get(instIndex);
            phyRelListOfInst.add(phyOperation);
            if (phyRelListOfInst.size() > maxPhyRelIndexOfOneInst) {
                maxPhyRelIndexOfOneInst = phyRelListOfInst.size();
            }
        }

        for (int relIdx = 0; relIdx < maxPhyRelIndexOfOneInst; ++relIdx) {
            for (int instIdx = 0; instIdx < instPhyRelArrList.size(); ++instIdx) {
                List<RelNode> phyRelArr = instPhyRelArrList.get(instIdx);
                if (relIdx < phyRelArr.size()) {
                    newInputs.add(phyRelArr.get(relIdx));
                }
            }
        }

        return newInputs;
    }

    public static boolean isPowerOfTwo(int val) {
        return (val & -val) == val;
    }

    public static int partition(int hashCode, int partitionCount, boolean isPowerOfTwo) {
        if (isPowerOfTwo) {
//            hashCode = HashCommon.mix(hashCode) >>> (Integer.numberOfLeadingZeros(partitionCount) + 1);
            hashCode = HashCommon.murmurHash3(hashCode);
            return hashCode & (partitionCount - 1);
        } else {
            hashCode = HashCommon.murmurHash3(hashCode) & Integer.MAX_VALUE; //ensure positive
            return hashCode % partitionCount;
        }
//        return Math.abs(HashCommon.murmurHash3(hashCode)) % partitionCount;
    }

    /**
     * Null-safe duplicate checker method
     *
     * @param current Existence
     * @param toBeCheck To be check
     * @return Duplicated or not
     */
    public static <T> boolean duplicated(Map<GroupKey, T> current, GroupKey toBeCheck,
                                         BiConsumer<GroupKey, T> computeIfPresent) {
        final boolean existsNull = Arrays.stream(toBeCheck.getGroupKeys()).anyMatch(Objects::isNull);

        if (existsNull) {
            return false;
        }

        return null != current.computeIfPresent(toBeCheck, (k, v) -> {
            computeIfPresent.accept(k, v);
            return v;
        });
    }

    /**
     * Null-safe duplicate checker method
     *
     * @param current Existence
     * @param toBeCheck To be check
     * @return Duplicated or not
     */
    public static boolean duplicated(Set<GroupKey> current, GroupKey toBeCheck) {
        final boolean existsNull = Arrays.stream(toBeCheck.getGroupKeys()).anyMatch(Objects::isNull);

        if (existsNull) {
            return false;
        }

        return current.contains(toBeCheck);
    }

    public static boolean duplicated(GroupKey current, GroupKey toBeCheck) {
        return duplicated(current, toBeCheck, false);
    }

    /**
     * duplicate checker method
     *
     * @param current Existence
     * @param toBeCheck To be check
     * @param nullSafeEqual True if we use <=> for compare
     * @return Duplicated or not
     */
    public static boolean duplicated(GroupKey current, GroupKey toBeCheck, boolean nullSafeEqual) {
        if (!nullSafeEqual) {
            final boolean existsNull = Arrays.stream(toBeCheck.getGroupKeys()).anyMatch(Objects::isNull);

            if (existsNull) {
                return false;
            }
        }

        return current.equals(toBeCheck);
    }

    /**
     * duplicate checker method
     *
     * @param current Existence
     * @param toBeCheck To be Check
     * @return True if exists duplicated group key
     */
    public static boolean duplicatedRow(List<GroupKey> current, List<GroupKey> toBeCheck) {
        boolean duplicated = false;
        for (Ord<GroupKey> o : Ord.zip(toBeCheck)) {
            duplicated = duplicated(current.get(o.i), o.getValue());

            if (duplicated) {
                break;
            }
        }
        return duplicated;
    }

    public static List<GroupKey> buildGroupKeys(List<List<Integer>> ukColumnsList,
                                                List<List<ColumnMeta>> ukColumnMetas,
                                                Function<Integer, Object> columnValue) {
        final List<GroupKey> replaceRow = new ArrayList<>();
        for (Ord<List<Integer>> o : Ord.zip(ukColumnsList)) {
            final List<Integer> columns = o.getValue();
            final List<ColumnMeta> metas = ukColumnMetas.get(o.i);
            final Object[] groupKeys = columns.stream().map(columnValue).toArray();

            replaceRow.add(new GroupKey(groupKeys, metas));
        }
        return replaceRow;
    }

    public static List<List<GroupKey>> buildRowDuplicateCheckers(List<List<Object>> selectedRows,
                                                                 List<List<Integer>> ukColumnsList,
                                                                 List<List<ColumnMeta>> ukColumnMetas) {
        final List<List<GroupKey>> duplicateCheckers = new ArrayList<>();
        selectedRows.forEach(row -> duplicateCheckers.add(buildGroupKeys(ukColumnsList, ukColumnMetas, row::get)));
        return duplicateCheckers;
    }

    public static Integer getDuplicatedCheckerRowIndex(List<GroupKey> row, List<List<GroupKey>> checkers) {
        Integer checkerIndex = -1;
        for (Ord<List<GroupKey>> ord : Ord.zip(checkers)) {
            final List<GroupKey> checkerRow = ord.getValue();

            for (Ord<GroupKey> o : Ord.zip(row)) {
                boolean duplicatedRow = duplicated(checkerRow.get(o.i), o.getValue());

                if (duplicatedRow) {
                    checkerIndex = ord.getKey();
                    break;
                }
            }

            if (checkerIndex >= 0) {
                break;
            }
        }
        return checkerIndex;
    }

    /**
     * @return 返回uk和它的值空间的映射
     */
    public static List<Set<GroupKey>> buildColumnDuplicateCheckers(List<List<Object>> duplicateValues,
                                                                   List<List<Integer>> ukColumnsList,
                                                                   List<List<ColumnMeta>> ukColumnMetas) {
        final List<Set<GroupKey>> duplicateCheckers = new ArrayList<>();
        for (Ord<List<Integer>> o : Ord.zip(ukColumnsList)) {
            final List<Integer> ukColumns = o.getValue();
            final List<ColumnMeta> metas = ukColumnMetas.get(o.i);

            final Set<GroupKey> checker = new HashSet<>();
            duplicateValues.forEach(row -> {
                final Object[] groupKeys = ukColumns.stream().map(row::get).toArray();
                checker.add(new GroupKey(groupKeys, metas));
            });

            duplicateCheckers.add(checker);
        }
        return duplicateCheckers;
    }

    public static long getMaxRowCount(RelNode node, ExecutionContext context) {
        long outputCount = Long.MAX_VALUE;
        if (node != null && node instanceof Sort) {
            Sort sort = (Sort) node;
            Map<Integer, ParameterContext> params = context.getParams().getCurrentParameter();
            if (((Sort) node).fetch != null) {
                outputCount = CBOUtil.getRexParam(sort.fetch, params);
                if (sort.offset != null) {
                    outputCount += CBOUtil.getRexParam(sort.offset, params);
                }
            }
        }
        return outputCount;
    }

    public static LogicalView convertToLogicalView(BaseTableOperation tableOperation) {
        RelNode relNode = tableOperation.getParent();
        if (relNode == null) {
            throw new RuntimeException("Don't support " + tableOperation + " convertTo LogicalView");
        }

        List<String> tableNameList;
        String schemaName = tableOperation.getSchemaName();
        if (tableOperation instanceof DirectTableOperation) {
            tableNameList = ((DirectTableOperation) tableOperation).getTableNames();
        } else if (tableOperation instanceof SingleTableOperation) {
            tableNameList = ((SingleTableOperation) tableOperation).getTableNames();
        } else if (tableOperation instanceof PhyTableOperation) {
            tableNameList = ((PhyTableOperation) tableOperation).getLogicalTableNames();
            if (tableNameList == null && tableOperation.getParent() instanceof DirectTableOperation) {
                tableNameList = ((DirectTableOperation) tableOperation.getParent()).getTableNames();
                if (schemaName == null) {
                    schemaName = ((DirectTableOperation) tableOperation.getParent()).getSchemaName();
                }
            }
        } else {
            throw new RuntimeException("Don't support " + tableOperation + " convertTo LogicalView");
        }

        LogicalView logicalView = null;
        if (relNode instanceof LogicalView) {
            logicalView = ((LogicalView) relNode).copy(relNode.getTraitSet());
            logicalView.setFromTableOperation(tableOperation);
        } else {
            final RelOptSchema catalog = RelUtils.buildCatalogReader(schemaName,
                PlannerContext.getPlannerContext(tableOperation.getCluster()).getExecutionContext());
            final RelOptTable table = catalog.getTableForMember(
                ImmutableList.of(schemaName, tableNameList.get(0)));
            logicalView = LogicalView.create(relNode, table);
//            if (tableOperation.getNativeSqlNode() != null) {
//                logicalView.setSqlTemplate(tableOperation.getNativeSqlNode());
//            }
        }
        logicalView.setTableName(tableNameList);
        logicalView.setFromTableOperation(tableOperation);
        return logicalView;
    }

    public static void addAllHosts(Optional<StageInfo> stageInfo, Set<String> collector) {
        stageInfo.ifPresent(stage -> {
            stageInfo.get().getTasks().stream().forEach(
                t -> {
                    String hostPort = t.getTaskStatus().getSelf().getNodeServer().toString();
                    collector.add(hostPort);
                }
            );
            stage.getSubStages().stream()
                .forEach(subStage -> addAllHosts(Optional.ofNullable(subStage), collector));
        });
    }

    public static Set<String> getQuerySchedulerHosts(ExecutionContext context) {
        Set<String> hosts = new LinkedHashSet<>();
        // coordinator
        hosts.add(ServiceProvider.getInstance().getServer().getLocalNode().getHostPort());
        if (ExecUtils.isMppMode(context)) {
            QueryInfo queryInfo = ServiceProvider.getInstance().getServer().getQueryManager().getQueryInfo(
                context.getTraceId());
            if (queryInfo != null && queryInfo.getOutputStage().isPresent()) {
                addAllHosts(queryInfo.getOutputStage(), hosts);
            } else {
                hosts.add("NULL");
            }
        }
        return hosts;
    }

    public static void getRootFailedTask(Optional<StageInfo> stageInfo, AtomicReference<TaskInfo> fail) {
        stageInfo.ifPresent(stage -> {
            stageInfo.get().getTasks().stream().forEach(
                t -> {
                    if (t.getTaskStatus().getState().isException()) {
                        if (fail.get() != null) {
                            if (fail.get().getStats().getEndTime().getMillis() > t.getStats().getEndTime()
                                .getMillis()) {
                                fail.set(t);
                            }
                        } else {
                            fail.set(t);
                        }
                    }
                }
            );
            stage.getSubStages().stream()
                .forEach(subStage -> getRootFailedTask(Optional.ofNullable(subStage), fail));
        });
    }

    public static boolean isMppMode(ExecutionContext context) {
        return context.getExecuteMode() == ExecutorMode.MPP;
    }

    public static boolean isTpMode(ExecutionContext context) {
        return !(context.getExecuteMode() == ExecutorMode.MPP || context.getExecuteMode() == ExecutorMode.AP_LOCAL);
    }

    public static boolean existMppOnlyInstanceNode() {
        InternalNodeManager nodeManager = ServiceProvider.getInstance().getServer().getNodeManager();
        Set<InternalNode> nodes = null;
        if (ConfigDataMode.isMasterMode()) {
            nodes = nodeManager.getAllNodes().getOtherActiveNodes();
        } else {
            nodes = nodeManager.getAllNodes().getActiveNodes();
        }
        return nodes != null && nodes.size() > 0;
    }

    public static boolean convertBuildSide(Join join) {
        boolean convertBuildSide = false;
        if (join instanceof HashJoin) {
            convertBuildSide = ((HashJoin) join).isOuterBuild();
        } else if (join instanceof HashGroupJoin) {
            convertBuildSide = true;
        }
        return convertBuildSide;
    }

    public static String genSubQueryTraceId(ExecutionContext context) {
        return context.getTraceId() + "_" + context.getSubqueryId();
    }

    public static boolean hasLeadership(String schema) {
        if (ServiceProvider.getInstance().getServer() != null) {
            InternalNode node = ServiceProvider.getInstance().getServer().getLocalNode();
            return node != null && node.isLeader();
        } else {
            return false;
        }
    }

    public static String getLeaderKey(String schema) {
        InternalNodeManager manager = ServiceProvider.getInstance().getServer().getNodeManager();
        if (manager != null) {
            List<Node> coordinators = manager.getAllCoordinators();
            if (coordinators != null && !coordinators.isEmpty()) {
                for (Node node : coordinators) {
                    if (node.isLeader()) {
                        return node.getHost() + TddlNode.SEPARATOR_INNER + node.getPort();
                    }
                }
            }
        }
        return null;
    }

    public static NodeStatusManager getStatusManager(String schema) {
        return ServiceProvider.getInstance().getServer().getStatusManager();
    }

    public static void syncNodeStatus(String schema) {
        try {
            IGmsSyncAction action = new RefreshNodeSyncAction(schema);
            SyncManagerHelper.sync(action, schema);
        } catch (Exception e) {
            logger.warn("node sync error", e);
        }
    }

    public static List<OrderByOption> convertFrom(List<RelFieldCollation> sortList) {
        List<OrderByOption> orderBys = new ArrayList<>(sortList.size());
        for (int i = 0, n = sortList.size(); i < n; i++) {
            RelFieldCollation field = sortList.get(i);
            orderBys
                .add(new OrderByOption(field.getFieldIndex(), field.direction, field.nullDirection));
        }
        return orderBys;
    }

    public static boolean isSQLMetricEnabled(ExecutionContext context) {
        if (context == null) {
            return true;
        }
        return MetricLevel.isSQLMetricEnabled(context.getParamManager().getInt(ConnectionParams.MPP_METRIC_LEVEL));
    }

    public static boolean isPipelineMetricEnabled(ExecutionContext context) {
        if (context == null) {
            return true;
        }
        return MetricLevel.isPipelineMetricEnabled(context.getParamManager().getInt(ConnectionParams.MPP_METRIC_LEVEL));
    }

    public static boolean isOperatorMetricEnabled(ExecutionContext context) {
        if (context == null) {
            return true;
        }
        return MetricLevel.isOperatorMetricEnabled(context.getParamManager().getInt(ConnectionParams.MPP_METRIC_LEVEL));
    }

    public static void getLsn(
        TopologyHandler topologyHandler, String group, Map<String, Long> lsnMap)
        throws SQLException {
        try (IConnection masterConn = topologyHandler.get(group, true).getDataSource().getConnection(
            MasterSlave.MASTER_ONLY);
            Statement stmt = masterConn.createStatement()) {
            ResultSet result =
                stmt.executeQuery("SELECT LAST_APPLY_INDEX FROM information_schema.ALISQL_CLUSTER_LOCAL");
            if (result.next()) {
                long masterLsn = Long.parseLong(result.getString(1));
                lsnMap.put(group, masterLsn);
            } else {
                throw new SQLException("Empty result while getting Applied_index");
            }
        }
    }

    public static void getLsn(
        Map.Entry<String, String> group, Map<String, Long> lsnMap) throws SQLException {
        getLsn(ExecutorContext.getContext(group.getValue()).getTopologyExecutor().getTopology(),
            group.getKey(), lsnMap);
    }

    public static void checkException(ListenableFuture<?> future) {
        Object ret = getFutureValue(future);
        if (ret instanceof Throwable) {
            throw new RuntimeException((Throwable) ret);
        }
    }

    public static void tryAndCheckException(ListenableFuture<?> future) {
        Object ret = tryGetFutureValue(future).orElse(null);
        if (ret instanceof Throwable) {
            throw new RuntimeException((Throwable) ret);
        }
    }

    public static boolean closeJdbcStreaming(
        Connection con, ResultSet rs, boolean killStreaming, boolean lessMy56Version) throws SQLException {

        throw new AssertionError("unreachable");
    }
}
