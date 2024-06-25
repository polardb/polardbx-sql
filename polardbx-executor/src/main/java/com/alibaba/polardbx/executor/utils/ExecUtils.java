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

import com.alibaba.polardbx.common.TddlNode;
import com.alibaba.polardbx.common.async.AsyncTask;
import com.alibaba.polardbx.common.constants.SequenceAttribute;
import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.jdbc.IDataSource;
import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;
import com.alibaba.polardbx.common.jdbc.MasterSlave;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.model.Group;
import com.alibaba.polardbx.common.model.RepoInst;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.properties.MetricLevel;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.AsyncUtils;
import com.alibaba.polardbx.common.utils.ExecutorMode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.bloomfilter.ConcurrentIntBloomFilter;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.version.InstanceVersion;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.druid.sql.ast.SqlType;
import com.alibaba.polardbx.executor.chunk.Chunk;
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
import com.alibaba.polardbx.executor.spi.IGroupExecutor;
import com.alibaba.polardbx.executor.spi.ITopologyExecutor;
import com.alibaba.polardbx.executor.sync.CollectVariableSyncAction;
import com.alibaba.polardbx.executor.spi.ITransactionManager;
import com.alibaba.polardbx.executor.sync.CollectVariableSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.gms.ha.impl.StorageHaManager;
import com.alibaba.polardbx.gms.ha.impl.StorageInstHaContext;
import com.alibaba.polardbx.gms.metadb.MetaDbConnectionProxy;
import com.alibaba.polardbx.gms.metadb.table.FilesAccessor;
import com.alibaba.polardbx.gms.metadb.table.FilesRecord;
import com.alibaba.polardbx.gms.node.GmsNodeManager;
import com.alibaba.polardbx.gms.node.InternalNode;
import com.alibaba.polardbx.gms.node.InternalNodeManager;
import com.alibaba.polardbx.gms.node.Node;
import com.alibaba.polardbx.gms.node.NodeStatusManager;
import com.alibaba.polardbx.gms.sync.IGmsSyncAction;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.gms.topology.InstConfigAccessor;
import com.alibaba.polardbx.gms.topology.InstConfigRecord;
import com.alibaba.polardbx.gms.topology.ServerInstIdManager;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.group.jdbc.DataSourceWrapper;
import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.alibaba.polardbx.gms.util.MetaDbLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.expression.ISelectable;
import com.alibaba.polardbx.optimizer.core.expression.bean.NullValue;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.BaseQueryOperation;
import com.alibaba.polardbx.optimizer.core.rel.BaseTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.BroadcastTableModify;
import com.alibaba.polardbx.optimizer.core.rel.DirectMultiDBTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.DirectShardingKeyTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.DirectTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.HashGroupJoin;
import com.alibaba.polardbx.optimizer.core.rel.HashJoin;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
import com.alibaba.polardbx.optimizer.core.rel.LogicalModify;
import com.alibaba.polardbx.optimizer.core.rel.LogicalModifyView;
import com.alibaba.polardbx.optimizer.core.rel.LogicalRelocate;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.OSSTableScan;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.SemiHashJoin;
import com.alibaba.polardbx.optimizer.core.rel.SingleTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.UnionOptHelper;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import com.alibaba.polardbx.optimizer.core.rel.dal.PhyShow;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.utils.GroupConnId;
import com.alibaba.polardbx.optimizer.utils.IColumnarTransaction;
import com.alibaba.polardbx.optimizer.utils.IDistributedTransaction;
import com.alibaba.polardbx.optimizer.utils.ITransaction;
import com.alibaba.polardbx.optimizer.utils.OptimizerUtils;
import com.alibaba.polardbx.optimizer.utils.PhyTableOperationUtil;
import com.alibaba.polardbx.optimizer.utils.QueryConcurrencyPolicy;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.rpc.pool.XConnection;
import com.alibaba.polardbx.sequence.Sequence;
import com.alibaba.polardbx.sequence.exception.SequenceException;
import com.alibaba.polardbx.sequence.impl.BaseSequence;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
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
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.util.Pair;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.weakref.jmx.internal.guava.primitives.Bytes;

import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.alibaba.polardbx.common.properties.ConnectionParams.MASTER_READ_WEIGHT;
import static com.alibaba.polardbx.common.utils.thread.ThreadCpuStatUtil.NUM_CORES;
import static com.alibaba.polardbx.executor.gsi.utils.Transformer.buildBatchParam;
import static com.alibaba.polardbx.executor.utils.failpoint.FailPointKey.FP_INJECT_IGNORE_INTERRUPTED_TO_STATISTIC_SCHEDULE_JOB;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.MoreFutures.tryGetFutureValue;

public class ExecUtils {

    private static final Logger logger = LoggerFactory.getLogger(ExecUtils.class);

    private static final int MAX_PARALLELISM = 16;
    public static byte[] hintPrefix = "/*DRDS /".getBytes(StandardCharsets.UTF_8);
    public static byte[] hintDivision = "/".getBytes(StandardCharsets.UTF_8);
    public static byte[] hintEnd = "/ */".getBytes(StandardCharsets.UTF_8);
    public static byte[] hintNULL = "null".getBytes(StandardCharsets.UTF_8);

    /**
     * get a mapping from instance id (host:port) to a list of group data sources
     */
    public static Map<String, List<TGroupDataSource>> getInstId2GroupList(Collection<String> schemaNames) {

        final Map<String, List<TGroupDataSource>> instId2GroupList = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

        for (String schemaName : schemaNames) {
            final List<String> groupNames =
                ExecutorContext.getContext(schemaName).getTopologyHandler().getAllTransGroupList();
            for (String groupName : groupNames) {

                final TGroupDataSource groupDataSource =
                    (TGroupDataSource) ExecutorContext.getContext(schemaName).getTopologyExecutor()
                        .getGroupExecutor(groupName).getDataSource();

                String instanceId = groupDataSource.getMasterSourceAddress();

                if (instanceId == null) {
                    continue;
                }

                final List<TGroupDataSource> groupList =
                    instId2GroupList.computeIfAbsent(instanceId, k -> new ArrayList<>());
                groupList.add(groupDataSource);
            }
        }

        return instId2GroupList;
    }

    public static Map<String, List<TGroupDataSource>> getInstId2GroupList(String schemaName) {
        return getInstId2GroupList(ImmutableList.of(schemaName));
    }

    /**
     * find full logical table name (`logical schema`.`logical table`) given physical DB and physical tables
     *
     * @param physicalTableMap (input) Map: physical DB -> physical table list
     * @param physicalToLogical (output) Map: `physical DB`.`physical table` -> `logical schema`.`logical table`
     */
    public static void updatePhysicalToLogical(Map<String, Set<String>> physicalTableMap,
                                               Map<String, String> physicalToLogical) {
        if (MapUtils.isEmpty(physicalTableMap)) {
            return;
        }
        final Set<String> allSchemaNames = OptimizerContext.getActiveSchemaNames();
        int counter = 0;
        for (final String schemaName : allSchemaNames) {
            final OptimizerContext optimizerContext = OptimizerContext.getContext(schemaName);

            if (null == optimizerContext) {
                continue;
            }

            final TddlRuleManager ruleManager = optimizerContext.getRuleManager();

            if (null == ruleManager) {
                continue;
            }

            final List<String> groupNames =
                ExecutorContext.getContext(schemaName).getTopologyHandler().getAllTransGroupList();

            for (final String groupName : groupNames) {
                final TGroupDataSource groupDataSource =
                    (TGroupDataSource) ExecutorContext.getContext(schemaName).getTopologyExecutor()
                        .getGroupExecutor(groupName).getDataSource();

                final Map<String, DataSourceWrapper> dataSourceWrapperMap =
                    groupDataSource.getConfigManager().getDataSourceWrapperMap();

                // get physical db name
                for (final DataSourceWrapper dataSourceWrapper : dataSourceWrapperMap.values()) {
                    final String physicalDbName = dataSourceWrapper
                        .getWrappedDataSource()
                        .getDsConfHandle()
                        .getRunTimeConf()
                        .getDbName();
                    final Set<String> physicalTables = physicalTableMap.get(physicalDbName);

                    if (null == physicalTables) {
                        continue;
                    }

                    for (final String physicalTable : physicalTables) {
                        final String fullyQualifiedPhysicalTableName =
                            (groupName + "." + physicalTable).toLowerCase();
                        final Set<String> logicalTableNames =
                            ruleManager.getLogicalTableNames(fullyQualifiedPhysicalTableName, schemaName);
                        if (CollectionUtils.isNotEmpty(logicalTableNames)) {
                            final String physical = String.format("`%s`.`%s`", physicalDbName, physicalTable);
                            final String logical =
                                String.format("`%s`.`%s`", schemaName, logicalTableNames.iterator().next());
                            physicalToLogical.put(physical, logical);
                        }
                    }

                    counter++;
                    if (counter == physicalTableMap.size()) {
                        // already find all physical DB
                        return;
                    }
                }
            }
        }
    }

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

    public static boolean needPutIfAbsent(ExecutionContext context, String key) {
        boolean ret = true;
        if (context.getHintCmds().containsKey(key)) {
            ret = false;
        } else {
            Properties cnProperties =
                MetaDbInstConfigManager.getInstance().getCnVariableConfigMap();
            if (cnProperties.containsKey(key)) {
                ret = false;
            }
        }
        return ret;
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

    public static int assignPartitionToExecutor(int counter, int allPartition, int partition, int executorSize) {
        if (allPartition < executorSize) {
            int fullGroup = executorSize / allPartition;
            int leftExecutor = executorSize % allPartition;
            int selectSize = fullGroup + ((partition < leftExecutor) ? 1 : 0);
            int selectSeq = counter % selectSize;
            return allPartition * selectSeq + partition;
        } else {
            return partition % executorSize;
        }
    }

    /**
     * Calculate how many degrees of parallelism each hash table has.
     * The hash table number = MIN(allPartition, executorSize)
     *
     * @param allPartition total number of partitions.
     * @param executorSize total degrees of parallelism.
     * @return degrees of parallelism each hash table has.
     */
    public static List<Integer> assignPartitionToExecutor(int allPartition, int executorSize) {
        List<Integer> assignResult = new ArrayList<>();
        if (allPartition < executorSize) {
            int fullGroup = executorSize / allPartition;
            List<Integer> fullPartList = IntStream.range(0, allPartition)
                .boxed()
                .collect(Collectors.toList());
            IntStream.range(0, fullGroup).forEach(t -> assignResult.addAll(fullPartList));
            int leftExecutor = executorSize % allPartition;
            List<Integer> leftPartList = IntStream.range(0, leftExecutor)
                .boxed()
                .collect(Collectors.toList());
            assignResult.addAll(leftPartList);
        } else {
            for (int part = 0; part < allPartition; part++) {
                assignResult.add(part % executorSize);
            }
        }
        return assignResult;
    }

    /**
     * Calculate how many partitions each hash table has.
     * The hash table number = MIN(allPartition, executorSize)
     *
     * @param allPartition total number of partitions.
     * @param executorSize total degrees of parallelism.
     * @return number of partitions each hash table has.
     */
    public static int[] partitionsOfEachBucket(int allPartition, int executorSize) {
        final int hashTableNum = Math.min(allPartition, executorSize);
        int[] results = new int[hashTableNum];

        if (allPartition <= executorSize) {
            for (int i = 0; i < hashTableNum; i++) {
                results[i] = 1;
            }
        } else {
            // hashTableNum = executorSize = parallelism
            for (int partIndex = 0; partIndex < allPartition; partIndex++) {
                final int hashTableIndex = partIndex % hashTableNum;
                results[hashTableIndex]++;
            }
        }
        return results;
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
            masterSlaveVal = MasterSlave.MASTER_ONLY;
        } else if (ConfigDataMode.isMasterMode() && (ec.getSqlType() != SqlType.SELECT && !ExecUtils.isMppMode(ec))) {
            //FIXME MPP SqlType is null now!
            masterSlaveVal = MasterSlave.MASTER_ONLY;
        } else if (ec.isInternalSystemSql()) {
            masterSlaveVal = MasterSlave.MASTER_ONLY;
        } else if (ec != null && ec.getExtraCmds() != null) {
            if (ec.getExtraCmds().containsKey(ConnectionProperties.MASTER)) {
                masterSlaveVal = MasterSlave.MASTER_ONLY;
            } else if (ec.getExtraCmds().containsKey(ConnectionProperties.SLAVE)) {
                masterSlaveVal = MasterSlave.SLAVE_ONLY;
            } else if (ec.getExtraCmds().containsKey(ConnectionProperties.FOLLOWER)) {
                masterSlaveVal = MasterSlave.FOLLOWER_ONLY;
            } else if (ec.getExtraCmds().containsKey(ConnectionProperties.FOLLOWER)) {
                masterSlaveVal = MasterSlave.FOLLOWER_ONLY;
            } else {
                masterSlaveVal = getMasterSlaveByWeight(ec);
            }
        } else {
            masterSlaveVal = getMasterSlaveByWeight(ec);
        }
        return masterSlaveVal;
    }

    private static MasterSlave getMasterSlaveByWeight(ExecutionContext ec) {
        MasterSlave ret = null;
        if (!ConfigDataMode.isMasterMode()) {
            ret = MasterSlave.SLAVE_ONLY;
        } else if (!ServerInstIdManager.getInstance().getAllHTAPReadOnlyInstIdSet().isEmpty() ||
            DynamicConfig.getInstance().enableFollowReadForPolarDBX()) {
            int readMasterWeight = ec.getParamManager().getInt(MASTER_READ_WEIGHT);
            if (readMasterWeight >= 100 || readMasterWeight < 0) {
                return MasterSlave.MASTER_ONLY;
            } else {
                if (Math.random() * 100 < readMasterWeight) {
                    ret = MasterSlave.MASTER_ONLY;
                } else {
                    ret = MasterSlave.SLAVE_ONLY;
                }
            }
        } else {
            //当不存在允许备库读能力资源时，则应该路由给主库
            ret = MasterSlave.MASTER_ONLY;
        }

        if (ret == MasterSlave.SLAVE_ONLY && ConfigDataMode.isMasterMode()) {
            int executeStrategy = ec.getParamManager().getInt(ConnectionParams.DELAY_EXECUTION_STRATEGY);
            if (executeStrategy == 2) {
                //all slave is delay, so can't continue use slave connection!
                ret = MasterSlave.LOW_DELAY_SLAVE_ONLY;
            } else if (executeStrategy == 1) {
                //all slave is delay, so change to master
                ret = MasterSlave.SLAVE_FIRST;
            } else {
                ret = MasterSlave.SLAVE_ONLY;
            }
        }
        return ret;
    }

    public static QueryConcurrencyPolicy getQueryConcurrencyPolicy(ExecutionContext executionContext) {
        return getQueryConcurrencyPolicy(executionContext, null);
    }

    public static QueryConcurrencyPolicy getQueryConcurrencyPolicy(ExecutionContext executionContext,
                                                                   LogicalView logicalView) {
        if (logicalView instanceof OSSTableScan) {
            if (executionContext.getParamManager().getBoolean(ConnectionParams.OSS_FILE_CONCURRENT)) {
                return QueryConcurrencyPolicy.FILE_CONCURRENT;
            } else {
                return QueryConcurrencyPolicy.CONCURRENT;
            }
        }

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
            if (logicalView != null && (logicalView.pushedRelNodeIsSort() || executionContext.getParamManager()
                .getBoolean(ConnectionParams.MERGE_CONCURRENT))) {
                return QueryConcurrencyPolicy.CONCURRENT;
            }
            if (executionContext.getParamManager().getBoolean(ConnectionParams.ENABLE_GROUP_PARALLELISM)) {
                return QueryConcurrencyPolicy.RELAXED_GROUP_CONCURRENT;
            }
            return QueryConcurrencyPolicy.GROUP_CONCURRENT_BLOCK;
        }

        if (executionContext.getParamManager().getBoolean(ConnectionParams.BLOCK_CONCURRENT)) {
            return QueryConcurrencyPolicy.CONCURRENT;
        }

        return QueryConcurrencyPolicy.SEQUENTIAL;
    }

    public static List<Map<Integer, ParameterContext>> getReturningResultByCursors(List<Cursor> cursors,
                                                                                   boolean isBroadcast) {
        List<Map<Integer, ParameterContext>> result = new ArrayList<>();
        try {
            for (Cursor cursor : cursors) {
                if (isBroadcast) {
                    result = buildBatchParam(cursor, false);
                } else {
                    result.addAll(buildBatchParam(cursor, false));
                }
            }
        } finally {
            for (Cursor inputCursor : cursors) {
                if (inputCursor != null) {
                    inputCursor.close(new ArrayList<>());
                }
            }
        }
        return result;
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
        } else if (relNode instanceof DirectShardingKeyTableOperation) {
            group = executionContext.getDbIndexAndTableName().getKey();
        } else if (relNode instanceof DirectMultiDBTableOperation) {
            group = ((DirectMultiDBTableOperation) relNode).getBaseDbIndex(executionContext);
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
        LogicalView logicalPlan, ExecutionContext executionContext, boolean forceIgnoreRF, SqlSelect sqlTemplate) {
        return logicalPlan.getInput(
            getUnionOptHelper(logicalPlan, executionContext), executionContext, forceIgnoreRF, sqlTemplate);
    }

    public static List<RelNode> getInputs(
        LogicalView logicalPlan, ExecutionContext executionContext, boolean forceIgnoreRF) {
        return logicalPlan.getInput(
            getUnionOptHelper(logicalPlan, executionContext), executionContext, forceIgnoreRF);
    }

    public static UnionOptHelper getUnionOptHelper(LogicalView logicalPlan, ExecutionContext executionContext) {
        return new UnionOptHelper() {
            @Override
            public int calMergeUnionSize(int totalCount, int toUnionCount, String groupName) {
                // mock mode force union size=1
                if (ConfigDataMode.isFastMock()) {
                    return 1;
                }

                if (logicalPlan instanceof OSSTableScan) {
                    return 1;
                }

                // Only one sql, do not need to union.
                if (toUnionCount == 1) {
                    return 0;
                }

                if (logicalPlan instanceof OSSTableScan) {
                    return 1;
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

                // todo JINWU: adjust union policy in AP queries, which should be done in another proper way
//                if (executionContext.getWorkloadType() == WorkloadType.AP) {
//                    // to exploit parallelism for AP queries, don't use union
//                    return 1;
//                }

                int unionSize = totalCount / getPrefetchNumForLogicalView(totalCount);
                int minUnionSize = Math.max(unionSize,
                    executionContext.getParamManager().getInt(ConnectionParams.MIN_MERGE_UNION_SIZE));
                int maxUnionSize = executionContext.getParamManager().getInt(ConnectionParams.MAX_MERGE_UNION_SIZE);
                minUnionSize = Math.min(minUnionSize, maxUnionSize);
                // union size should be no more than toUnionCount
                return Math.min(toUnionCount, minUnionSize);
            }
        };
    }

    public static boolean useExplicitTransaction(ExecutionContext context) {
        return OptimizerUtils.useExplicitTransaction(context);

    }

    public static boolean allowMultipleReadConns(ExecutionContext context, LogicalView logicalView) {
        return OptimizerUtils.allowMultipleReadConns(context, logicalView);
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

    public static byte[] buildDRDSTraceCommentBytes(ExecutionContext context) {
        String clientIp = context.getClientIp();
        String traceId = context.getTraceId();
        Object server_id = "";
        Long phySqlId = context.getPhySqlId();
        final Map<String, Object> extraVariables = context.getExtraServerVariables();
        if (null != extraVariables && extraVariables.containsKey("polardbx_server_id")) {
            server_id = extraVariables.get("polardbx_server_id");
        }
        if (clientIp == null) {
            clientIp = "null";
        }
        return Bytes.concat(hintPrefix, clientIp.getBytes(StandardCharsets.UTF_8), hintDivision,
            traceId.getBytes(StandardCharsets.UTF_8), hintDivision,
            phySqlId == null ? hintNULL : phySqlId.toString().getBytes(StandardCharsets.UTF_8),
            hintDivision, server_id.toString().getBytes(StandardCharsets.UTF_8), hintEnd);

    }

    public static Sequence mockSeq(String name) {
        BaseSequence sequence = new BaseSequence() {
            long seq = 0;

            @Override
            public long nextValue() throws SequenceException {
                long value = seq++;
                currentValue = value;
                return value;
            }

            @Override
            public long nextValue(int size) throws SequenceException {
                long value = seq + size;
                currentValue = value;
                return value;
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
                                     int[] positionLinks, int[] hashCodeResults, int[] intermediates,
                                     int[] blockHashCodes,
                                     ConcurrentIntBloomFilter bloomFilter, int[] ignoreNullBlocks,
                                     int ignoreNullBlocksSize) {
        // Calculate hash codes of the whole chunk
        keyChunk.hashCodeVector(hashCodeResults, intermediates, blockHashCodes, keyChunk.getPositionCount());

        if (checkJoinKeysAllNullSafe(keyChunk, ignoreNullBlocks, ignoreNullBlocksSize)) {
            // If all keys are not null, we can leave out the null-check procedure
            for (int offset = 0; offset < keyChunk.getPositionCount(); offset++, position++) {
                int next = hashTable.put(position, hashCodeResults[offset]);
                positionLinks[position] = next;
                if (bloomFilter != null) {
                    bloomFilter.putInt(hashCodeResults[offset]);
                }
            }
        } else {
            // Otherwise we have to check nullability for each row
            for (int offset = 0; offset < keyChunk.getPositionCount(); offset++, position++) {
                if (checkJoinKeysNulSafe(keyChunk, offset, ignoreNullBlocks, ignoreNullBlocksSize)) {
                    int next = hashTable.put(position, hashCodeResults[offset]);
                    positionLinks[position] = next;
                    if (bloomFilter != null) {
                        bloomFilter.putInt(hashCodeResults[offset]);
                    }
                }
            }
        }
    }

    public static void buildOneChunk(Chunk keyChunk, int position, ConcurrentRawHashTable hashTable,
                                     int[] positionLinks, int[] hashCodeResults, int[] intermediates,
                                     int[] blockHashCodes,
                                     ConcurrentIntBloomFilter bloomFilter, List<Integer> ignoreNullBlocks) {
        // Calculate hash codes of the whole chunk
        keyChunk.hashCodeVector(hashCodeResults, intermediates, blockHashCodes, keyChunk.getPositionCount());

        if (checkJoinKeysAllNullSafe(keyChunk, ignoreNullBlocks)) {
            // If all keys are not null, we can leave out the null-check procedure
            for (int offset = 0; offset < keyChunk.getPositionCount(); offset++, position++) {
                int next = hashTable.put(position, hashCodeResults[offset]);
                positionLinks[position] = next;
                if (bloomFilter != null) {
                    bloomFilter.putInt(hashCodeResults[offset]);
                }
            }
        } else {
            // Otherwise we have to check nullability for each row
            for (int offset = 0; offset < keyChunk.getPositionCount(); offset++, position++) {
                if (checkJoinKeysNulSafe(keyChunk, offset, ignoreNullBlocks)) {
                    int next = hashTable.put(position, hashCodeResults[offset]);
                    positionLinks[position] = next;
                    if (bloomFilter != null) {
                        bloomFilter.putInt(hashCodeResults[offset]);
                    }
                }
            }
        }
    }

    public static void buildOneChunk(Chunk keyChunk, int position, ConcurrentRawHashTable hashTable,
                                     int[] positionLinks,
                                     ConcurrentIntBloomFilter bloomFilter, List<Integer> ignoreNullBlocks) {
        // Calculate hash codes of the whole chunk
        int[] hashes = keyChunk.hashCodeVector();

        if (checkJoinKeysAllNullSafe(keyChunk, ignoreNullBlocks)) {
            // If all keys are not null, we can leave out the null-check procedure
            for (int offset = 0; offset < keyChunk.getPositionCount(); offset++, position++) {
                int next = hashTable.put(position, hashes[offset]);
                positionLinks[position] = next;
                if (bloomFilter != null) {
                    bloomFilter.putInt(hashes[offset]);
                }
            }
        } else {
            // Otherwise we have to check nullability for each row
            for (int offset = 0; offset < keyChunk.getPositionCount(); offset++, position++) {
                if (checkJoinKeysNulSafe(keyChunk, offset, ignoreNullBlocks)) {
                    int next = hashTable.put(position, hashes[offset]);
                    positionLinks[position] = next;
                    if (bloomFilter != null) {
                        bloomFilter.putInt(hashes[offset]);
                    }
                }
            }
        }
    }

    public static boolean checkJoinKeysAllNullSafe(Chunk keyChunk, int[] ignoreNullBlocks, int size) {
        for (int i = 0; i < size; i++) {
            if (keyChunk.getBlock(ignoreNullBlocks[i]).mayHaveNull()) {
                return false;
            }
        }
        return true;
    }

    public static boolean checkJoinKeysNulSafe(Chunk keyChunk, int offset, int[] ignoreNullBlocks, int size) {
        for (int i = 0; i < size; i++) {
            if (keyChunk.getBlock(ignoreNullBlocks[i]).isNull(offset)) {
                return false;
            }
        }
        return true;
    }

    public static boolean checkJoinKeysAllNullSafe(Chunk keyChunk, List<Integer> ignoreNullBlocks) {
        for (int i : ignoreNullBlocks) {
            if (keyChunk.getBlock(i).mayHaveNull()) {
                return false;
            }
        }
        return true;
    }

    public static boolean checkJoinKeysNulSafe(Chunk keyChunk, int offset, List<Integer> ignoreNullBlocks) {
        for (int i : ignoreNullBlocks) {
            if (keyChunk.getBlock(i).isNull(offset)) {
                return false;
            }
        }
        return true;
    }

    /**
     * reorder the  inputList by both groupName and groupConnId.
     * (the count of groupConnId of a group depend on the params of GROUP_PARALLELISM)
     * <pre>
     *     for example, set GROUP_PARALLELISM=2 ：
     *      inputs:
     *              p1(g1,conn1),p3(g1,conn1),p5(g1,conn2),p6(g2,conn1),
     *              p2(g2,conn1),p4(g2,conn2),p7(g2,conn2),p8(g1,conn2)
     *      inputs after zigzag:
     *          ( g1.conn1 and g1.conn2 can be exec concurrently )
     *
     *           orderNum       g1.conn1, g2.conn1, g1.conn2, g2.conn2
     *              0             p1,p3
     *              1                       p2,p6
     *              2                                p5,p8
     *              3                                           p4,p7
     *
     *      return
     *              { {p1,p3}/g1.conn1, {p2,p6}/g2.conn1, {p5,p8}/g1.conn2, {p4,p7}/g2.conn2 }
     * </pre>
     */
    public static List<RelNode> zigzagInputsByBothDnInstAndGroupConnId(List<RelNode> inputs,
                                                                       String schemaName, ExecutionContext ec,
                                                                       List<GroupConnId> outputGrpConnIdSet,
                                                                       List<List<RelNode>> outputPhyOpListGroupedByGrpConnId) {
        if (inputs.isEmpty()) {
            return new ArrayList<>();
        }
        RelNode firstOp = inputs.get(0);
        boolean isPhyTblOp = firstOp instanceof PhyTableOperation;
        boolean isSystemDb = SystemDbHelper.isDBBuildIn(schemaName);
        if (!isPhyTblOp || isSystemDb) {
            throw new NotSupportException(
                "Not support do zizag by group conn id for non PhyTableOperations or build-in db");
        }

        Map<String, RepoInst> groupToDnInstMap =
            ExecutorContext.getContext(schemaName).getTopologyHandler().getGroupRepoInstMapsForzigzag();

        PhyTableOperation phyOperation;
        Long grpParallelism = ec.getGroupParallelism();
        Boolean enableGrpParallelism = ec.getParamManager().getBoolean(ConnectionParams.ENABLE_GROUP_PARALLELISM);

        /**
         * key: groupConnid
         * val: list of phyOp
         *
         */
        Map<GroupConnId, List<RelNode>> grpConnToPhyOpSetMap = new HashMap<>();
        int maxPhyOpCntOfOneGroupConn = 0;

        /**
         * key: groupName
         * val: set of GroupConnId
         */
        Map<String, List<GroupConnId>> grpToConnIdListMap = new HashMap<>();
        Map<String, Set<GroupConnId>> grpToConnIdSetMap = new HashMap<>();//used to remove duplicate GroupConnId
        int maxGrpConnCntOfOneGrp = 0;

        /**
         * key: dnInstId( dnId@dnAddr )
         * val: set of GroupName
         */
        Map<String, List<String>> dnInstIdToGroupListMap = new HashMap<>();
        Map<String, Set<String>> dnInstIdToGroupSetMap = new HashMap<>();//used to remove duplicate groupName
        int maxGrpCntOfOneDnInst = 0;

        for (int i = 0; i < inputs.size(); i++) {
            phyOperation = (PhyTableOperation) inputs.get(i);
            String groupIndex = phyOperation.getDbIndex();
            RepoInst dnInst = groupToDnInstMap.get(groupIndex);
            String dnRepoId = dnInst.getRepoInstId();
            Long connId = enableGrpParallelism ?
                PhyTableOperationUtil.computeGrpConnIdByGrpConnKey(
                    PhyTableOperationUtil.fetchPhyOpIntraGroupConnKey(phyOperation, ec), enableGrpParallelism,
                    grpParallelism) : PhyTableOperationUtil.DEFAULT_WRITE_CONN_ID;
            GroupConnId groConn = new GroupConnId(groupIndex, connId);
            List<RelNode> phyOpSet = grpConnToPhyOpSetMap.computeIfAbsent(groConn, conn -> new ArrayList<>());
            phyOpSet.add(phyOperation);
            if (maxPhyOpCntOfOneGroupConn < phyOpSet.size()) {
                maxPhyOpCntOfOneGroupConn = phyOpSet.size();
            }
            List<GroupConnId> connIdList = grpToConnIdListMap.computeIfAbsent(groupIndex, g -> new ArrayList<>());
            Set<GroupConnId> connIdSet = grpToConnIdSetMap.computeIfAbsent(groupIndex, g -> new HashSet<>());
            if (!connIdSet.contains(groConn)) {
                connIdList.add(groConn);
                connIdSet.add(groConn);
                if (maxGrpConnCntOfOneGrp < connIdList.size()) {
                    maxGrpConnCntOfOneGrp = connIdList.size();
                }
            }
            List<String> grpList = dnInstIdToGroupListMap.computeIfAbsent(dnRepoId, id -> new ArrayList<>());
            Set<String> grpSet = dnInstIdToGroupSetMap.computeIfAbsent(dnRepoId, id -> new HashSet<>());
            if (!grpSet.contains(groupIndex)) {
                grpList.add(groupIndex);
                grpSet.add(groupIndex);
                if (maxGrpCntOfOneDnInst < grpList.size()) {
                    maxGrpCntOfOneDnInst = grpList.size();
                }
            }
        }

        /**
         * step1:
         *
         * dn1: g1,g3
         * dn2: g2,g4
         * => reorder group set by dnInst
         * g1,g2,g3,g4
         */
        List<String> newGroupListAfterZigzagDnInst = new ArrayList<>();
        for (int i = 0; i < maxGrpCntOfOneDnInst; i++) {
            for (List<String> grpSetItem : dnInstIdToGroupListMap.values()) {
                if (i < grpSetItem.size()) {
                    newGroupListAfterZigzagDnInst.add(grpSetItem.get(i));
                }
            }
        }

        /**
         * step2:
         *
         * g1: c1,c2
         * g2: c2
         * g3: c1
         * g4: c2
         * => reorder group connId set by new ordered group name set
         * g1c1,g2c2,g3c1,g4c2,g1c2
         *
         * step1 & step2: do dnInst zigzag for the list of groupConnId
         * <pre>
         *     old:
         *          dn1: g1.conn1, g2.conn1,g1.conn3
         *          dn2: g3.conn3, g4.conn1
         *          dn3: g5.conn2
         *     new
         *          orderNum        dn1            dn2          dn3
         *          1               g1.conn1
         *          2                              g3.conn3
         *          3                                           g5.conn2
         *          4               g4.conn1
         *          5                              g2.conn1
         *          6               g1.conn3
         *     ,so return
         *      { g1.conn1, g3.conn3, g5.conn2, g4.conn1, g2.conn1, g1.conn3 }
         *
         * </pre>
         */
        List<GroupConnId> newGroupConnListAfterZigzagDnInstAndGrp = new ArrayList<>();
        for (int i = 0; i < maxGrpConnCntOfOneGrp; i++) {

            /**
             * foreach the new order of group name set
             */
            for (int j = 0; j < newGroupListAfterZigzagDnInst.size(); j++) {
                List<GroupConnId> grpConnIdSet = grpToConnIdListMap.get(newGroupListAfterZigzagDnInst.get(j));
                if (i < grpConnIdSet.size()) {
                    newGroupConnListAfterZigzagDnInstAndGrp.add(grpConnIdSet.get(i));
                }
            }
        }

        /**
         * set step3:
         *
         * g1c1: op1,op6
         * g2c2: op2,op7
         * g3c1: op3,op8
         * g4c2: op4,op9
         * g1c2: op5,op10
         * => reorder all phyOp set by new ordered group connId set
         * op1,op2,op3,op4,op5,op6,op7,op8,op9,op10
         *
         * step3 do grpConnId zigzag for the list of all phy op
         * <pre>
         *     for example, set GROUP_PARALLELISM=2 ：
         *      mapping:
         *          g1.conn1: p1,p3
         *          g3.conn3: p7
         *          g5.conn2: p15
         *          g4.conn1: p12
         *          g2.conn1: p2,p6
         *          g1.conn3: p5
         *
         *      inputs after zigzag:
         *          ( g1.conn1 and g1.conn2 can be exec concurrently )
         *
         *           dn             dn1,      dn2,      dn3,      dn1,      dn2,      dn1
         *           orderNum       g1.conn1, g3.conn3, g5.conn2, g4.conn1, g2.conn1, g1.conn3
         *              0             p1,p3
         *              1                       p7
         *              2                                p15
         *              3                                           p12
         *              4                                                   p2,p6
         *              6                                                               p5
         *
         *      return
         *              { {p1,p3}/g1.conn1, {p7}/g3.conn3, {p15}/g5.conn2, {p12}/g4.conn1, {p2,p6}/g2.conn1, p5/g1.conn3 }
         *
         * </pre>
         *
         */
        List<RelNode> newInputListAfterZigzagDnAndGrpCnnId = new ArrayList<>();
        for (int i = 0; i < maxPhyOpCntOfOneGroupConn; i++) {

            /**
             * foreach the new order of group connId set
             */
            for (int j = 0; j < newGroupConnListAfterZigzagDnInstAndGrp.size(); j++) {
                List<RelNode> phyOpSet = grpConnToPhyOpSetMap.get(newGroupConnListAfterZigzagDnInstAndGrp.get(j));
                if (i < phyOpSet.size()) {
                    newInputListAfterZigzagDnAndGrpCnnId.add(phyOpSet.get(i));
                }
            }
        }
        if (outputGrpConnIdSet != null) {
            outputGrpConnIdSet.addAll(newGroupConnListAfterZigzagDnInstAndGrp);
        }
        if (outputPhyOpListGroupedByGrpConnId != null) {
            for (int i = 0; i < newGroupConnListAfterZigzagDnInstAndGrp.size(); i++) {
                List<RelNode> phyOpSet = grpConnToPhyOpSetMap.get(newGroupConnListAfterZigzagDnInstAndGrp.get(i));
                outputPhyOpListGroupedByGrpConnId.add(phyOpSet);
            }
        }
        return newInputListAfterZigzagDnAndGrpCnnId;
    }

    /**
     * 将inputList按RDS实例重排一下
     */
    public static List<RelNode> zigzagInputsByMysqlInst(List<RelNode> inputs, String schemaName, ExecutionContext ec) {

        List<RelNode> newInputs = new ArrayList<RelNode>(inputs.size());

        if (inputs.isEmpty()) {
            return newInputs;
        }

        RelNode firstOp = inputs.get(0);
        boolean isPhyTblOp = firstOp instanceof PhyTableOperation;
        boolean isSystemDb = SystemDbHelper.isDBBuildIn(schemaName);

        if (isPhyTblOp && !isSystemDb) {
            /**
             *    dn1        dn2        dn3
             * { grp1.conn1,grp2.conn2,grp1.conn2,grp2.conn2 ... }
             */
            newInputs = ExecUtils.zigzagInputsByBothDnInstAndGroupConnId(inputs, schemaName, ec, null, null);
            return newInputs;
        } else {
            BaseQueryOperation phyOperation;
            Map<String, RepoInst> groupRepoInstMap =
                ExecutorContext.getContext(schemaName).getTopologyHandler().getGroupRepoInstMapsForzigzag();

            /**
             *  all phy op list of each db inst:
             *
             *    =======dn1======  =====dn2=====
             *  { { op1, op3,.... }, {op2,op4,...}, ... }
             */
            List<List<RelNode>> instPhyRelArrList = new ArrayList<List<RelNode>>();

            /**
             * mapping: dbInstAddr -> dnInstIndex of instPhyRelArrList
             */
            Map<String, Integer> instIndexMap = new HashMap<String, Integer>();

            int maxPhyRelIndexOfOneInst = 0;
            int instCount = 0;
            for (int i = 0; i < inputs.size(); i++) {
                phyOperation = (BaseQueryOperation) inputs.get(i);
                String groupIndex = phyOperation.getDbIndex();
                RepoInst repoInst = groupRepoInstMap.get(groupIndex);
                String instId = repoInst.getRepoInstId();
                Integer instIndex = instIndexMap.get(instId);

                List<RelNode> phyRelListOfInst = null;
                if (instIndex == null) {
                    ++instCount;
                    instIndex = instCount - 1;
                    instIndexMap.put(instId, instIndex);
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
    }

    public static int reorderGroupsByDnId(Collection<String> groups, String schema,
                                          ConcurrentLinkedQueue<IDataSource> reorderedQueue) {
        // Reorder groups by DN inst id.
        // For example, group 0-8 in DN0, group 9-15 in DN1,
        // before reorder: group 0, 1, 2, ..., 9, 10, ..., 15
        // after reorder: group 0, 9, 1, 10, 2, 11, ..., 8, 15
        // DN inst id -> all group data sources in that DN.
        Map<String, List<IDataSource>> instIdToDataSources = new HashMap<>();
        for (String group : groups) {
            TGroupDataSource dataSource =
                (TGroupDataSource) ExecutorContext.getContext(schema).getTopologyHandler().get(group).getDataSource();
            String instId = dataSource.getMasterSourceAddress();
            List<IDataSource> datasourceList = instIdToDataSources.computeIfAbsent(instId, o -> new ArrayList<>());
            datasourceList.add(dataSource);
        }
        List<Iterator<IDataSource>> dnIterators =
            instIdToDataSources.values().stream().map(List::iterator).collect(Collectors.toList());
        // Simple k-way merge.
        while (!dnIterators.isEmpty()) {
            // Iterator of all DN.
            Iterator<Iterator<IDataSource>> it = dnIterators.iterator();
            while (it.hasNext()) {
                // Iterator of all group data sources in one DN.
                Iterator<IDataSource> dnDatasource = it.next();
                if (dnDatasource.hasNext()) {
                    reorderedQueue.offer(dnDatasource.next());
                    dnDatasource.remove();
                } else {
                    // This DN is done.
                    it.remove();
                }
            }
        }

        return instIdToDataSources.size();
    }

    public static boolean isPowerOfTwo(int val) {
        return (val & -val) == val;
    }

    public static int partition(int hashCode, int partitionCount, boolean isPowerOfTwo) {
        if (isPowerOfTwo) {
            hashCode = HashCommon.murmurHash3(hashCode);
            return hashCode & (partitionCount - 1);
        } else {
            hashCode = HashCommon.murmurHash3(hashCode) & Integer.MAX_VALUE; //ensure positive
            return hashCode % partitionCount;
        }
    }

    public static int partitionUnderPairWise(long hashCode, int partitionCount, int fullPartCount,
                                             boolean isFullPartPowerOfTwo) {
        if (isFullPartPowerOfTwo) {
            return (int) ((hashCode & (fullPartCount - 1)) % partitionCount);
        } else {
            return (int) (((hashCode & Long.MAX_VALUE) % fullPartCount) % partitionCount);
        }
    }

    public static int calcStoragePartNum(long hashCode, int fullPartCount, boolean isFullPartPowerOfTwo) {
        if (isFullPartPowerOfTwo) {
            return (int) (hashCode & (fullPartCount - 1));
        } else {
            return (int) ((hashCode & Long.MAX_VALUE) % fullPartCount);
        }
    }

    public static int directPartition(long hashCode, int partitionCount, boolean isPowerOfTwo) {
        if (isPowerOfTwo) {
            return (int) (hashCode & (partitionCount - 1));
        } else {
            return (int) ((hashCode & Long.MAX_VALUE) % partitionCount);
        }
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

    public static List<GroupKey> buildNewGroupKeys(List<List<Integer>> ukColumnsList,
                                                   List<List<ColumnMeta>> ukColumnMetas,
                                                   Function<Integer, Object> columnValue,
                                                   ExecutionContext ec) {
        final List<GroupKey> replaceRow = new ArrayList<>();
        for (Ord<List<Integer>> o : Ord.zip(ukColumnsList)) {
            final List<Integer> columns = o.getValue();
            final List<ColumnMeta> metas = ukColumnMetas.get(o.i);
            final Object[] groupKeys = columns.stream().map(columnValue).toArray();

            replaceRow.add(new NewGroupKey(groupKeys, metas, false, ec));
        }
        return replaceRow;
    }

    public static List<GroupKey> buildNewGroupKeys(List<List<Integer>> ukColumnsList,
                                                   List<List<ColumnMeta>> ukColumnMetas,
                                                   List<Object> columnValue,
                                                   List<RexNode> rex,
                                                   ExecutionContext ec) {
        final List<GroupKey> replaceRow = new ArrayList<>();
        for (Ord<List<Integer>> o : Ord.zip(ukColumnsList)) {
            final List<Integer> columns = o.getValue();
            final List<ColumnMeta> metas = ukColumnMetas.get(o.i);
            final Object[] groupKeys = columns.stream().map(columnValue::get).toArray();
            final List<RexNode> rexs = columns.stream().map(rex::get).collect(Collectors.toList());

            replaceRow.add(new NewGroupKey(groupKeys, metas, rexs, false, ec));
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

            final Set<GroupKey> checker = new TreeSet<>();
            duplicateValues.forEach(row -> {
                final Object[] groupKeys = ukColumns.stream().map(row::get).toArray();
                checker.add(new GroupKey(groupKeys, metas));
            });

            duplicateCheckers.add(checker);
        }
        return duplicateCheckers;
    }

    public static List<Set<GroupKey>> buildColumnDuplicateCheckersWithNewGroupKey(List<List<Object>> duplicateValues,
                                                                                  List<List<Integer>> ukColumnsList,
                                                                                  List<List<ColumnMeta>> ukColumnMetas,
                                                                                  ExecutionContext ec) {
        final List<Set<GroupKey>> duplicateCheckers = new ArrayList<>();
        for (Ord<List<Integer>> o : Ord.zip(ukColumnsList)) {
            final List<Integer> ukColumns = o.getValue();
            final List<ColumnMeta> metas = ukColumnMetas.get(o.i);

            final Set<GroupKey> checker = new HashSet<>();
            duplicateValues.forEach(row -> {
                final Object[] groupKeys = ukColumns.stream().map(row::get).toArray();
                checker.add(new NewGroupKey(groupKeys, metas, false, ec));
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
                long offset = 0;
                if (sort.offset != null) {
                    offset = CBOUtil.getRexParam(sort.offset, params);
                }
                if (outputCount == Long.MAX_VALUE || offset == Long.MAX_VALUE) {
                    outputCount = Long.MAX_VALUE;
                } else {
                    outputCount = outputCount + offset;
                }
            }
        }
        return outputCount;
    }

    public static LogicalView convertToLogicalView(
        BaseTableOperation tableOperation, ExecutionContext context) {
        RelNode relNode = tableOperation.getParent();
        if (relNode == null) {
            throw new RuntimeException("Don't support " + tableOperation + " convertTo LogicalView");
        }

        List<String> tableNameList;
        String schemaName;
        if (tableOperation instanceof DirectMultiDBTableOperation) {
            schemaName = ((DirectMultiDBTableOperation) tableOperation).getBaseSchemaName(context);
        } else {
            schemaName = tableOperation.getSchemaName();
        }

        if (tableOperation instanceof DirectTableOperation) {
            tableNameList = (tableOperation).getLogicalTableNames();
        } else if (tableOperation instanceof DirectMultiDBTableOperation) {
            tableNameList = ((DirectMultiDBTableOperation) tableOperation).getLogicalTables(schemaName);
        } else if (tableOperation instanceof SingleTableOperation) {
            tableNameList = (tableOperation).getLogicalTableNames();
        } else if (tableOperation instanceof DirectShardingKeyTableOperation) {
            tableNameList = (tableOperation).getLogicalTableNames();
        } else if (tableOperation instanceof PhyTableOperation) {
            tableNameList = (tableOperation).getLogicalTableNames();
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
        hosts.add(ServiceProvider.getInstance().getServer().getLocalNode().getHostMppPort());
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
                            if (fail.get().getTaskStats().getEndTime().getMillis() > t.getTaskStats().getEndTime()
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
        if (ServiceProvider.getInstance().getServer() == null) {
            return false;
        }
        InternalNodeManager nodeManager = ServiceProvider.getInstance().getServer().getNodeManager();
        Set<InternalNode> nodes = null;
        if (ConfigDataMode.isMasterMode()) {
            nodes = nodeManager.getAllNodes().getOtherActiveNodes();
        } else {
            nodes = nodeManager.getAllNodes().getActiveNodes();
        }
        return nodes != null && nodes.size() > 0;
    }

    public static int getActiveNodeCount() {
        if (ServiceProvider.getInstance().getServer() == null) {
            return 1;
        }
        InternalNodeManager nodeManager = ServiceProvider.getInstance().getServer().getNodeManager();
        return Math.max(nodeManager.getAllNodes().getActiveNodes().size(), 1);
    }

    public static boolean convertBuildSide(Join join) {
        boolean convertBuildSide = false;
        if (join instanceof HashJoin) {
            convertBuildSide = ((HashJoin) join).isOuterBuild();
        } else if (join instanceof HashGroupJoin) {
            convertBuildSide = true;
        } else if (join instanceof SemiHashJoin) {
            convertBuildSide = ((SemiHashJoin) join).isOuterBuild();
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
            SyncManagerHelper.sync(action, schema, SyncScope.ALL);
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

    /**
     * @param tso Task to send a timestamp to MASTER storage nodes in order to keep their latest timestamp up-to-date.
     */
    public static long getLsn(IDataSource dataSource, long tso, String hint) throws SQLException {
        final String tsoSql;
        if (InstanceVersion.isMYSQL80()) {
            tsoSql = hint + "call dbms_xa.advance_gcn_no_flush(" + tso + ")";
        } else {
            tsoSql = hint + "SET GLOBAL innodb_heartbeat_seq = " + tso;
        }

        final String lsnSql = hint + "SELECT LAST_APPLY_INDEX FROM information_schema.ALISQL_CLUSTER_LOCAL";

        ResultSet result;
        try (IConnection masterConn = dataSource.getConnection(MasterSlave.MASTER_ONLY)) {
//            if (masterConn.isWrapperFor(XConnection.class)) {
//                masterConn.unwrap(XConnection.class).execUpdate(tsoSql, null, true);
//            }

            try (Statement stmt = masterConn.createStatement()) {
//                if (tso > 0) {
//                    if (masterConn.isWrapperFor(XConnection.class)) {
//                        result = stmt.executeQuery(lsnSql);
//                    } else {
//                        // Multi-statement, the first one is a SET statement, the last is a SELECT query.
//                        stmt.executeQuery(tsoSql + ";" + lsnSql);
//                        if (stmt.getUpdateCount() != -1 && stmt.getMoreResults()) {
//                            result = stmt.getResultSet();
//                        } else {
//                            throw new SQLException("Error occurs while getting Applied_index result set");
//                        }
//                    }
//                } else {
//                    result = stmt.executeQuery(lsnSql);
//                }
                result = stmt.executeQuery(lsnSql);

                if (result.next()) {
                    return Long.parseLong(result.getString(1));
                } else {
                    throw new SQLException("Empty result while getting Applied_index");
                }
            }
        }
    }


    public static long getLsn(IDataSource dataSource) throws SQLException {
        try (IConnection masterConn = dataSource.getConnection(MasterSlave.MASTER_ONLY);
            Statement stmt = masterConn.createStatement()) {
            ResultSet result =
                stmt.executeQuery("SELECT LAST_APPLY_INDEX FROM information_schema.ALISQL_CLUSTER_LOCAL");
            if (result.next()) {
                long masterLsn = Long.parseLong(result.getString(1));
                return masterLsn;
            } else {
                throw new SQLException("Empty result while getting Applied_index");
            }
        }
    }

    //FIXME optimize get LSN by Xprotocol way.
//    public static long getLsn(IDataSource dataSource) throws SQLException {
//        try (IConnection masterConn = dataSource.getConnection(MasterSlave.MASTER_ONLY);
//            Statement stmt = masterConn.createStatement()) {
//            ResultSet result =
//                stmt.executeQuery("call dbms_consensus.show_cluster_local()");
//            if (result.next()) {
//                long masterLsn = Long.parseLong(result.getString(9));
//                return masterLsn;
//            } else {
//                throw new SQLException("Empty result while getting Applied_index");
//            }
//        }
//    }

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

    /**
     * @return A set of DN's storage instance id
     */
    public static Set<String> getAllDnStorageId() {
        final Set<String> allDnId = new LinkedHashSet<>();
        final Map<String, StorageInstHaContext> storageInfo = StorageHaManager.getInstance().getStorageHaCtxCache();
        for (Entry<String, StorageInstHaContext> idAndContext : storageInfo.entrySet()) {
            final String storageInstanceId = idAndContext.getKey();
            final StorageInstHaContext context = idAndContext.getValue();
            if (context.isDNMaster()) {
                allDnId.add(storageInstanceId);
            }
        }
        return allDnId;
    }

    public static boolean isMysql80Version() {
        boolean isMysql80Version = false;
        try {
            isMysql80Version = ExecutorContext.getContext(
                SystemDbHelper.INFO_SCHEMA_DB_NAME).getStorageInfoManager().isMysql80();
        } catch (Throwable t) {
            //ignore
        }
        return isMysql80Version;
    }

    public static String getMysqlVersion() {
        String version = null;
        try {
            version = ExecutorContext.getContext(
                SystemDbHelper.INFO_SCHEMA_DB_NAME).getStorageInfoManager().getDnVersion();
        } catch (Throwable t) {
            //ignore
        }
        return version;
    }

    public static List<String> getTableGroupNames(String schemaName, String tableName, ExecutionContext ec) {
        final Set<String> dbNames;
        final TddlRuleManager or = Objects.requireNonNull(OptimizerContext.getContext(schemaName)).getRuleManager();
        if (DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
            PartitionInfo partitionInfo =
                ec.getSchemaManager(schemaName).getTable(tableName).getPartitionInfo();
            dbNames = partitionInfo.getTopology().keySet();
        } else {
            dbNames = or.getTableRule(tableName).getActualTopology().keySet();
        }
        return Lists.newArrayList(dbNames);
    }

    public static Pair<Integer, Integer> calculateLogicalAndPhysicalThread(ExecutionContext ec, int groupConnSetSize,
                                                                           boolean isSingleTable, boolean useTrans) {
        int logicalThreads = ec.getParamManager().getInt(ConnectionParams.MODIFY_SELECT_LOGICAL_THREADS);
        int physicalThreads =
            ec.getParamManager().getInt(ConnectionParams.MODIFY_SELECT_PHYSICAL_THREADS);
        //logicalThreads，physicalThreads <= 0 意味着根据环境自动设置
        if (physicalThreads <= 0) {
            int cnCores =
                ExecUtils.getPolarDBXCores(ec.getParamManager(), ConfigDataMode.isMasterMode());
            if (isSingleTable) {
                //单表情况，执行物理任务线程等于核数
                physicalThreads = Math.max(cnCores, 1);
            } else {
                //多表情况下，执行物理任务线程数等于核数*2，Insert的CPU开销较小
                physicalThreads = Math.max(cnCores, 1) * 2;
            }
        }
        if (useTrans && physicalThreads > groupConnSetSize) {
            //事务时执行物理任务线程不能超过group数目
            physicalThreads = groupConnSetSize;
        }
        if (logicalThreads <= 0) {
            //执行逻辑任务线程个数是执行物理任务线程的1/4，这个比例是经验值。
            logicalThreads = (physicalThreads % 4 > 0) ? physicalThreads / 4 + 1 : physicalThreads / 4;
        }
        return Pair.of(logicalThreads, physicalThreads);
    }

    /**
     * Is statistic sketch background job need to be interrupted
     */
    public static com.alibaba.polardbx.common.utils.Pair<Boolean, String> needSketchInterrupted() {
        if (FailPoint.isKeyEnable(FP_INJECT_IGNORE_INTERRUPTED_TO_STATISTIC_SCHEDULE_JOB)) {
            return com.alibaba.polardbx.common.utils.Pair.of(false, "FailPoint");
        }
        boolean enableStatisticBackground =
            InstConfUtil.getBool(ConnectionParams.ENABLE_BACKGROUND_STATISTIC_COLLECTION);
        if (!enableStatisticBackground) {
            return com.alibaba.polardbx.common.utils.Pair.of(true,
                "ENABLE_BACKGROUND_STATISTIC_COLLECTION not enabled");
        }
        if (!InstConfUtil.getBool(ConnectionParams.ENABLE_HLL)) {
            return com.alibaba.polardbx.common.utils.Pair.of(true, "ENABLE_HLL not enabled");
        }

        if (InstConfUtil.isInMaintenanceTimeWindow()) {
            return com.alibaba.polardbx.common.utils.Pair.of(false, "");
        } else {
            return com.alibaba.polardbx.common.utils.Pair.of(true, "not in maintenance time window");
        }
    }

    public static boolean useParameterDelegate(ExecutionContext context) {
        return isMppMode(context) && DynamicConfig.getInstance().useParameterDelegate();
    }

    /**
     * version: select @@polardbx_engine_version as version
     * releaseDate: select @@polardbx_release_date as release_date
     *
     * @return {Version}-{ReleaseDate}
     */
    public static String getDnPolardbVersion() throws Exception {
        String sql = MetaDbUtil.POLARDB_VERSION_SQL;
        Set<String> allDnId = ExecUtils.getAllDnStorageId();
        if (allDnId.isEmpty()) {
            throw new SQLException("Failed to get DN datasource");
        }
        String dnId = allDnId.iterator().next();
        try (Connection conn = DbTopologyManager.getConnectionForStorage(dnId);
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sql)) {
            String dnPolardbxVersion = null;
            String dnReleaseDate = null;
            if (rs.next()) {
                dnPolardbxVersion = rs.getString(1);
                dnReleaseDate = rs.getString(2);
            }
            return String.format("%s-%s", dnPolardbxVersion, dnReleaseDate);
        }
    }

    /**
     * Scan each DN, and find all prepared trx, and record the max trx id and min trx id of these trx.
     *
     * @param dnIds [in] Data nodes to scan.
     * @param executor [in] Executor to get connection of DN.
     * @param exceptions [out] Exceptions during scan.
     * @param minId [out] Min id of all prepared trx.
     * @param maxId [out] Max id of all prepared trx.
     */
    public static void scanRecoveredTrans(Set<String> dnIds, ITopologyExecutor executor,
                                          ConcurrentLinkedQueue<Exception> exceptions,
                                          AtomicLong minId, AtomicLong maxId) {
        List<Future> futures = new ArrayList<>();
        // Parallelism is the number of DN.
        for (String dnId : dnIds) {
            futures.add(executor.getExecutorService().submit(null, null, AsyncTask.build(() -> {
                try (Connection conn = DbTopologyManager.getConnectionForStorage(dnId);
                    Statement stmt = conn.createStatement()) {
                    if (conn.isWrapperFor(XConnection.class)) {
                        // Note: XA RECOVER will hold the LOCK_transaction_cache lock, so never block it.
                        conn.unwrap(XConnection.class).setDefaultTokenKb(Integer.MAX_VALUE);
                    }
                    ResultSet rs = stmt.executeQuery("XA RECOVER");
                    while (rs.next()) {
                        long formatId = rs.getLong(1);
                        int gtridLength = rs.getInt(2);
                        byte[] data = rs.getBytes(4);

                        if (formatId == 1) {
                            byte[] gtridData = Arrays.copyOfRange(data, 0, gtridLength);
                            if (checkGtridPrefix(gtridData)) {
                                int atSymbolIndex = ArrayUtils.indexOf(gtridData, (byte) '@');
                                long trxId = Long.parseLong(new String(gtridData, 5, atSymbolIndex - 5), 16);

                                // CAS to update min id.
                                long tmp = minId.get();
                                while (trxId < tmp && !minId.compareAndSet(tmp, trxId)) {
                                    tmp = minId.get();
                                }

                                // CAS to update max id.
                                tmp = maxId.get();
                                while (trxId > tmp && !maxId.compareAndSet(tmp, trxId)) {
                                    tmp = maxId.get();
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    exceptions.offer(e);
                }
            })));
        }
        AsyncUtils.waitAll(futures);
    }

    /**
     * Check whether begins with prefix 'drds-'
     */
    public static boolean checkGtridPrefix(byte[] data) {
        return data.length > 5
            && data[0] == 'd' && data[1] == 'r' && data[2] == 'd' && data[3] == 's' && data[4] == '-';
    }

    public static List<FilesRecord> getFilesMetaByNames(List<String> files) {
        try (final Connection connection = MetaDbUtil.getConnection()) {
            final FilesAccessor filesAccessor = new FilesAccessor();
            filesAccessor.setConnection(connection);
            connection.setAutoCommit(false);
            try {
                return filesAccessor.queryFilesByNames(files);
            } finally {
                connection.setAutoCommit(true);
            }
        } catch (Throwable t) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, t);
        }
    }

    /**
     * Wait some CN global variable in DynamicConfig changed in all CN nodes after setting global.
     *
     * @param k class member variable in DynamicConfig, in string format.
     * @param v expected value after setting global.
     * @param timeout wait timeout, in second.
     * @return null if success, or error message if failed.
     */
    public static String waitVarChange(String k, String v, long timeout) {
        logger.warn("Start waiting var changed: " + k + " -> " + v);
        int retry = 0, waitMilli = 1000;
        int maxRetry = (int) (timeout * 1000 / waitMilli);
        boolean success = true;
        while (retry < maxRetry) {
            success = true;
            retry++;
            try {
                CollectVariableSyncAction action = new CollectVariableSyncAction(k);
                List<List<Map<String, Object>>> values = SyncManagerHelper.sync(action, SyncScope.ALL);
                for (List<Map<String, Object>> value : values) {
                    String result = value.get(0).get("Value").toString();
                    if (!v.equalsIgnoreCase(result)) {
                        System.out.println(result);
                        success = false;
                        break;
                    }
                }
                if (success) {
                    break;
                } else {
                    Thread.sleep(waitMilli);
                }
            } catch (Throwable t) {
                return "Error occurs when waiting CN global variable changed: " + t.getMessage();
            }
        }
        logger.warn("Finish waiting var changed: " + k + " -> " + v + ", success: " + success);
        if (!success) {
            return "Timeout waiting CN global variable changed.";
        }
        return null;
    }

    /**
     * @return a columnar transaction using given tso.
     */
    public static ITransaction createColumnarTransaction(String schema, ExecutionContext ec, long tso) {
        ITransactionManager manager = ExecutorContext.getContext(schema).getTransactionManager();
        ITransaction trx =
            manager.createTransaction(ITransactionPolicy.TransactionClass.COLUMNAR_READ_ONLY_TRANSACTION, ec);
        if (trx instanceof IColumnarTransaction) {
            ((IColumnarTransaction) trx).setTsoTimestamp(tso);
        }
        return trx;
    }

    public static Runnable forceAllTrx2PC() throws SQLException, InterruptedException {
        // Force all trx being strict 2PC trx,
        // the following configs are expected to be true.
        String instId = InstIdUtil.getInstId();
        Set<String> paramKeys = ImmutableSet.of(
            ConnectionProperties.ENABLE_XA_TSO,
            ConnectionProperties.ENABLE_AUTO_COMMIT_TSO,
            ConnectionProperties.FORBID_AUTO_COMMIT_TRX
        );
        InstConfigAccessor accessor = new InstConfigAccessor();
        accessor.setConnection(MetaDbUtil.getConnection());
        // Original config.
        List<InstConfigRecord> records = accessor.queryByParamKeys(instId, paramKeys);
        // Need to be changed config.
        Set<String> needChangedConfigs = new HashSet<>(paramKeys);
        for (InstConfigRecord record : records) {
            if (needChangedConfigs.contains(record.paramKey.toUpperCase())
                && "true".equalsIgnoreCase(record.paramVal)) {
                needChangedConfigs.remove(record.paramKey.toUpperCase());
            }
        }
        // Changed config.
        List<InstConfigRecord> changedRecords = null;
        if (!needChangedConfigs.isEmpty()) {
            // Change these configs.
            Properties properties = new Properties();
            for (String changedRecord : needChangedConfigs) {
                properties.put(changedRecord, "true");
            }
            MetaDbUtil.setGlobal(properties);
            // Changed config.
            changedRecords = accessor.queryByParamKeys(instId, needChangedConfigs);
            waitVarChange("forbidAutoCommitTrx", "true", 5);
            // A better way to drain trx ?
            Thread.sleep(1000);
        }

        final AtomicBoolean recover = new AtomicBoolean(false);

        // Recover these changed configs.
        final List<InstConfigRecord> changedRecords0 = new ArrayList<>();
        if (null == changedRecords || changedRecords.isEmpty()) {
            // No need to recover.
            recover.set(true);
        } else {
            changedRecords0.addAll(changedRecords);
        }
        return () -> {
            if (!recover.compareAndSet(false, true)) {
                return;
            }
            // Restore var.
            List<InstConfigRecord> current = accessor.queryByParamKeys(instId, needChangedConfigs);
            Properties properties = new Properties();
            for (InstConfigRecord changedRecord : changedRecords0) {
                for (InstConfigRecord currentRecord : current) {
                    if (currentRecord.paramKey.equalsIgnoreCase(changedRecord.paramKey)) {
                        if (currentRecord.gmtModified.compareTo(changedRecord.gmtModified) <= 0) {
                            // Not changed since we modify it, recover it back.
                            properties.put(changedRecord.paramKey, "false");
                        }
                        break;
                    }
                }
            }
            try {
                MetaDbUtil.setGlobal(properties);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        };
    }
}
