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

package com.alibaba.polardbx.executor.mpp.split;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.BytesSql;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.common.TopologyHandler;
import com.alibaba.polardbx.executor.gms.ColumnarManager;
import com.alibaba.polardbx.common.oss.ColumnarPartitionPrunedSnapshot;
import com.alibaba.polardbx.executor.gms.DynamicColumnarManager;
import com.alibaba.polardbx.executor.gms.util.ColumnarTransactionUtils;
import com.alibaba.polardbx.executor.mpp.metadata.Split;
import com.alibaba.polardbx.executor.spi.IGroupExecutor;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.executor.utils.SubqueryUtils;
import com.alibaba.polardbx.gms.metadb.columnar.FlashbackColumnarManager;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.BaseQueryOperation;
import com.alibaba.polardbx.optimizer.core.rel.BaseTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.LookupSql;
import com.alibaba.polardbx.optimizer.core.rel.OSSTableScan;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableScanBuilder;
import com.alibaba.polardbx.optimizer.core.rel.util.DynamicParamInfo;
import com.alibaba.polardbx.optimizer.core.rel.util.IndexedDynamicParamInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.partition.pruning.PartPrunedResult;
import com.alibaba.polardbx.optimizer.partition.pruning.PhysicalPartitionInfo;
import com.alibaba.polardbx.optimizer.utils.GroupConnId;
import com.alibaba.polardbx.optimizer.utils.IColumnarTransaction;
import com.alibaba.polardbx.optimizer.utils.ITransaction;
import com.alibaba.polardbx.optimizer.utils.OptimizerUtils;
import com.alibaba.polardbx.optimizer.utils.PartitionUtils;
import com.alibaba.polardbx.optimizer.utils.PhyTableOperationUtil;
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import com.alibaba.polardbx.optimizer.utils.QueryConcurrencyPolicy;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.statistics.RuntimeStatistics;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelPartitionWise;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.lang.StringUtils;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.alibaba.polardbx.executor.mpp.split.OssSplit.NO_PARTITION_INFO;
import static com.alibaba.polardbx.group.jdbc.TGroupDataSource.LOCAL_ADDRESS;
import static com.alibaba.polardbx.optimizer.utils.QueryConcurrencyPolicy.CONCURRENT;
import static com.alibaba.polardbx.optimizer.utils.QueryConcurrencyPolicy.FILE_CONCURRENT;
import static com.alibaba.polardbx.optimizer.utils.QueryConcurrencyPolicy.FIRST_THEN_CONCURRENT;
import static com.alibaba.polardbx.optimizer.utils.QueryConcurrencyPolicy.GROUP_CONCURRENT_BLOCK;

public class SplitManagerImpl implements SplitManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(SplitManagerImpl.class);

    public static SqlNode DYNAMIC_CONDITION_PLACEHOLDER = new SqlBasicCall(SqlStdOperatorTable.EQUALS,
        new SqlNode[] {
            SqlLiteral.createCharString("bka_magic", SqlParserPos.ZERO),
            SqlLiteral.createCharString("bka_magic", SqlParserPos.ZERO)}, SqlParserPos.ZERO);

    @Override
    public SplitInfo getSingleSplit(LogicalView logicalView, ExecutionContext executionContext) {
        if (logicalView instanceof OSSTableScan) {
            throw GeneralUtil.nestedException("Impossible code path: oss table scan with single split");
        } else {
            return logicalViewSingleSplit(logicalView, executionContext);
        }
    }

    @Override
    public SplitInfo getSplits(
        LogicalView logicalView, ExecutionContext executionContext, boolean highConcurrencyQuery) {
        if (logicalView instanceof OSSTableScan) {
            RuntimeStatistics stat = (RuntimeStatistics) executionContext.getRuntimeStatistics();
            long startTimeNanos = System.nanoTime();
            SplitInfo splitInfo = ossTableScanSplit((OSSTableScan) logicalView, executionContext, highConcurrencyQuery);
            if (((OSSTableScan) logicalView).isColumnarIndex() && stat != null) {
                long timeCost = System.nanoTime() - startTimeNanos;
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(MessageFormat.format("timeCost = {0}, traceId = {1}, logicalView = {2}",
                        timeCost, executionContext.getTraceId(), logicalView));
                }
                stat.addColumnarSnapshotTimecost(timeCost);
            }
            return splitInfo;
        } else {
            return logicalViewSplit(logicalView, executionContext, highConcurrencyQuery);
        }
    }

    private SplitInfo logicalViewSingleSplit(LogicalView logicalView, ExecutionContext executionContext) {
        List<Split> splitList = new ArrayList<>();
        String schemaName = logicalView.getSchemaName();
        if (StringUtils.isEmpty(schemaName)) {
            schemaName = executionContext.getSchemaName();
        }
        TopologyHandler topology = ExecutorContext.getContext(schemaName).getTopologyHandler();

        ITransaction.RW rw = ITransaction.RW.READ;

        BaseQueryOperation queryOperation = logicalView.fromTableOperation();
        List<List<ParameterContext>> params = null;
        String dbIndex = null;

        if (queryOperation instanceof BaseTableOperation && ((BaseTableOperation) queryOperation).isForUpdate()) {
            rw = ITransaction.RW.WRITE;
        } else if (logicalView.getLockMode() == SqlSelect.LockMode.EXCLUSIVE_LOCK) {
            rw = ITransaction.RW.WRITE;
        }

        List<List<String>> phyTableNames = new ArrayList<>();
        boolean useParameterDelegate = ExecUtils.useParameterDelegate(executionContext);
        if (queryOperation instanceof PhyTableOperation) {
            PhyTableScanBuilder phyOperationBuilder =
                (PhyTableScanBuilder) ((PhyTableOperation) queryOperation).getPhyOperationBuilder();
            if (phyOperationBuilder != null) {
                List<List<String>> groupTables = ((PhyTableOperation) queryOperation).getTableNames();
                params = new ArrayList<>(groupTables.size());
                for (List<String> tables : groupTables) {
                    params.add(phyOperationBuilder.buildSplitParams(dbIndex, tables, useParameterDelegate));
                }
                dbIndex = queryOperation.getDbIndex();
                phyTableNames = groupTables;
            }
        }
        if (params == null) {

            Pair<String, Map<Integer, ParameterContext>> dbIndexAndParam =
                queryOperation
                    .getDbIndexAndParam(executionContext.getParams() == null ? null : executionContext.getParams()
                        .getCurrentParameter(), phyTableNames, executionContext);
            Map<Integer, ParameterContext> p = dbIndexAndParam.getValue();

            List<ParameterContext> splitParams = new ArrayList<>();
            params = Collections.singletonList(splitParams);
            if (p != null) {
                int paramCount = p.keySet().size();
                for (int i = 1; i <= paramCount; i++) {
                    splitParams.add(i - 1, p.get(i));
                }
            }
            dbIndex = dbIndexAndParam.getKey();

        }

        IGroupExecutor groupExecutor = topology.get(dbIndex);
        TGroupDataSource ds = (TGroupDataSource) groupExecutor.getDataSource();

        String address = LOCAL_ADDRESS;
        if (!DynamicConfig.getInstance().enableExtremePerformance()) {
            address = ds.getOneAtomAddress(ConfigDataMode.isMasterMode());
        }

        byte[] hint = ExecUtils.buildDRDSTraceCommentBytes(executionContext);
        BytesSql sqlTemplate = queryOperation.getBytesSql();
        Long intraGroupSortKey =
            PhyTableOperationUtil
                .fetchBaseOpIntraGroupConnKey(queryOperation, dbIndex, phyTableNames,
                    executionContext);
        final ByteString galaxyDigestBS = logicalView.getGalaxyPrepareDigest(executionContext, sqlTemplate);
        final byte[] galaxyDigest = null == galaxyDigestBS ? null : galaxyDigestBS.toByteArray();
        JdbcSplit split = new JdbcSplit(ds.getDbGroupKey(),
            schemaName,
            dbIndex,
            hint,
            sqlTemplate,
            null,
            params,
            address,
            ImmutableList.of(logicalView.getTableNames()),
            rw,
            false,
            intraGroupSortKey,
            galaxyDigest,
            galaxyDigest != null && logicalView.isSupportGalaxyPrepare(), null, null);
        splitList.add(new Split(false, split));

        HashMap<String, String> groups = new HashMap<>();
        groups.put(dbIndex, schemaName);
        Map<GroupConnId, String> grpConnIdSet = new HashMap<>();
        grpConnIdSet.put(new GroupConnId(dbIndex, split.getGrpConnId(executionContext)), schemaName);
        return new SplitInfo(logicalView.getRelatedId(), false, QueryConcurrencyPolicy.SEQUENTIAL,
            ImmutableList.of(splitList), groups, 1,
            1, false, grpConnIdSet);
    }

    private SplitInfo logicalViewSplit(LogicalView logicalView, ExecutionContext executionContext,
                                       boolean highConcurrencyQuery) {
        if (logicalView != null) {
            ITransaction.RW rw =
                logicalView.getLockMode() == SqlSelect.LockMode.UNDEF ? ITransaction.RW.READ : ITransaction.RW.WRITE;
            boolean underSort = logicalView.pushedRelNodeIsSort();
            QueryConcurrencyPolicy concurrencyPolicy = highConcurrencyQuery ? CONCURRENT :
                ExecUtils.getQueryConcurrencyPolicy(executionContext, logicalView);

            boolean allowMultipleReadConns = ExecUtils.allowMultipleReadConns(executionContext, logicalView);

            //在调度split之前，如果scan有子查询，则在server端计算完成
            List<RexDynamicParam> scalarList = logicalView.getScalarList();
            if (scalarList.size() > 0) {
                SubqueryUtils.buildScalarSubqueryValue(scalarList, executionContext); // handle
            }

            List<RelNode> inputs = ExecUtils.getInputs(
                logicalView, executionContext, !ExecUtils.isMppMode(executionContext));

            //FIXME 当优化器支持对memory的估算后，需要调用以下逻辑，预估内存如果超限，禁止执行SQL
            //HandlerCommon.checkExecMemCost(ExecutionContext executionContext, List<RelNode> subNodes);

            if (inputs.size() > 1) {
                /*
                 * 记录全表扫描，当前判断条件为访问分片数大于1，用于后续sql.log输出
                 */
                executionContext.setHasScanWholeTable(true);
            }

            byte[] hint = ExecUtils.buildDRDSTraceCommentBytes(executionContext);

            String schemaName = logicalView.getSchemaName();
            if (StringUtils.isEmpty(schemaName)) {
                schemaName = executionContext.getSchemaName();
            }

            TopologyHandler topology = ExecutorContext.getContext(logicalView.getSchemaName())
                .getTopologyHandler();

            HashMap<String, String> shardSet = new HashMap<>();
            Set<String> instSet = new HashSet<>();
            Map<GroupConnId, String> grpConnSet = new HashMap();
            int splitCount = 0;
            switch (concurrencyPolicy) {
            case SEQUENTIAL:
            case CONCURRENT:
                List<RelNode> sortInputs = ExecUtils.zigzagInputsByMysqlInst(inputs, schemaName, executionContext);
                List<Split> splitList = new ArrayList<>();
                for (RelNode input : sortInputs) {
                    JdbcSplit split =
                        parseRelNode(
                            logicalView, topology, input, logicalView.getSchemaName(), hint, rw, executionContext);
                    if (split != null) {
                        shardSet.put(split.getDbIndex(), split.getSchemaName());
                        instSet.add(split.getHostAddress());
                        grpConnSet.put(new GroupConnId(split.getDbIndex(), split.getGrpConnId(executionContext)),
                            split.getSchemaName());
                        splitList.add(new Split(false, split));
                        splitCount++;
                    }
                }
                return new SplitInfo(logicalView.getRelatedId(), logicalView.isExpandView(), concurrencyPolicy,
                    ImmutableList.of(splitList),
                    shardSet, instSet.size(),
                    splitCount, underSort, grpConnSet);
            case FIRST_THEN_CONCURRENT: //只有对广播表写的时候才会用到，在查询的时候我们恢复默认策略即可GROUP_CONCURRENT_BLOCK
            case GROUP_CONCURRENT_BLOCK:
                if (!allowMultipleReadConns) {
                    Map<String, List<Split>> splitAssignment = new HashMap<>();
                    Map<String, List<String>> instDbMap = new LinkedHashMap<>();
                    List<List<Split>> outList = new ArrayList<>();
                    int maxInstDbsize = 0;

                    for (RelNode input : inputs) {
                        JdbcSplit split =
                            parseRelNode(logicalView, topology, input, logicalView.getSchemaName(), hint, rw,
                                executionContext);
                        if (split != null) {
                            shardSet.put(split.getDbIndex(), split.getSchemaName());
                            instSet.add(split.getHostAddress());
                            grpConnSet.put(new GroupConnId(split.getDbIndex(), split.getGrpConnId(executionContext)),
                                split.getSchemaName());
                            if (!splitAssignment.containsKey(split.getDbIndex())) {
                                splitAssignment.put(split.getDbIndex(), new ArrayList<>());
                                if (!instDbMap.containsKey(split.getHostAddress())) {
                                    instDbMap.put(split.getHostAddress(), new ArrayList<>());
                                }
                                instDbMap.get(split.getHostAddress()).add(split.getDbIndex());
                                if (instDbMap.get(split.getHostAddress()).size() > maxInstDbsize) {
                                    maxInstDbsize = instDbMap.get(split.getHostAddress()).size();
                                }
                            }
                            splitAssignment.get(split.getDbIndex()).add(new Split(false, split));
                            splitCount++;
                        }
                    }
                    for (int i = 0; i < maxInstDbsize; i++) {
                        for (List<String> dbs : instDbMap.values()) {
                            if (i < dbs.size()) {
                                outList.add(splitAssignment.get(dbs.get(i)));
                            }
                        }
                    }
                    return new SplitInfo(logicalView.getRelatedId(), logicalView.isExpandView(), concurrencyPolicy,
                        outList.isEmpty() ? ImmutableList.of(new ArrayList<>()) : outList, shardSet,
                        instSet.size(),
                        splitCount, underSort, grpConnSet);
                } else {
                    List<RelNode> sortInputByInts =
                        ExecUtils.zigzagInputsByMysqlInst(inputs, schemaName, executionContext);
                    List<Split> retLists = new ArrayList<>();
                    for (RelNode input : sortInputByInts) {
                        JdbcSplit split =
                            parseRelNode(logicalView, topology, input, logicalView.getSchemaName(), hint, rw,
                                executionContext);
                        if (split != null) {
                            shardSet.put(split.getDbIndex(), split.getSchemaName());
                            grpConnSet.put(new GroupConnId(split.getDbIndex(), split.getGrpConnId(executionContext)),
                                split.getSchemaName());
                            instSet.add(split.getHostAddress());
                            retLists.add(new Split(false, split));
                            splitCount++;
                        }
                    }
                    return new SplitInfo(logicalView.getRelatedId(), logicalView.isExpandView(), GROUP_CONCURRENT_BLOCK,
                        ImmutableList.of(retLists),
                        shardSet,
                        instSet.size(), splitCount, underSort, grpConnSet);
                }
            case RELAXED_GROUP_CONCURRENT:

                if (!allowMultipleReadConns) {
                    /**
                     * the output of grpConnIdSet has been zigzag by mysqlInst
                     */
                    List<GroupConnId> grpConnIdSetOutput = new ArrayList<>();
                    List<List<RelNode>> newInputsGroupedByGrpConnIdOutput = new ArrayList<>();
                    ExecUtils.zigzagInputsByBothDnInstAndGroupConnId(inputs, schemaName, executionContext,
                        grpConnIdSetOutput, newInputsGroupedByGrpConnIdOutput);
                    List<List<Split>> outList = new ArrayList<>();
                    for (int i = 0; i < newInputsGroupedByGrpConnIdOutput.size(); i++) {
                        List<RelNode> phyOpListOfOneGrpConn = newInputsGroupedByGrpConnIdOutput.get(i);
                        List<Split> splits = new ArrayList<>();
                        for (int j = 0; j < phyOpListOfOneGrpConn.size(); j++) {
                            JdbcSplit split =
                                parseRelNode(logicalView, topology, phyOpListOfOneGrpConn.get(j),
                                    logicalView.getSchemaName(), hint, rw, executionContext);
                            if (split != null) {
                                shardSet.put(split.getDbIndex(), split.getSchemaName());
                                grpConnSet.put(grpConnIdSetOutput.get(i), split.getSchemaName());
                                instSet.add(split.getHostAddress());
                                splitCount++;
                                splits.add(new Split(false, split));
                            }
                        }
                        outList.add(splits);
                    }
                    return new SplitInfo(logicalView.getRelatedId(), logicalView.isExpandView(), concurrencyPolicy,
                        outList.isEmpty() ? ImmutableList.of(new ArrayList<>()) : outList, shardSet,
                        instSet.size(),
                        splitCount, underSort, grpConnSet);
                } else {
                    /**
                     * Come here means it is allowed to do table scan by multiple read conns
                     */
                    List<RelNode> sortInputByInts =
                        ExecUtils.zigzagInputsByMysqlInst(inputs, schemaName, executionContext);
                    List<Split> retLists = new ArrayList<>();
                    for (RelNode input : sortInputByInts) {
                        JdbcSplit split =
                            parseRelNode(logicalView, topology, input, logicalView.getSchemaName(), hint, rw,
                                executionContext);
                        if (split != null) {
                            shardSet.put(split.getDbIndex(), split.getSchemaName());
                            grpConnSet.put(new GroupConnId(split.getDbIndex(), split.getGrpConnId(executionContext)),
                                split.getSchemaName());
                            instSet.add(split.getHostAddress());
                            retLists.add(new Split(false, split));
                            splitCount++;
                        }
                    }
                    return new SplitInfo(logicalView.getRelatedId(), logicalView.isExpandView(),
                        QueryConcurrencyPolicy.RELAXED_GROUP_CONCURRENT,
                        ImmutableList.of(retLists),
                        shardSet,
                        instSet.size(), splitCount, underSort, grpConnSet);
                }

            case INSTANCE_CONCURRENT:
                //FIXME 现在并没有实例间并行的配置
                break;
            default:
                break;
            }
            throw new TddlRuntimeException(ErrorCode.ERR_GENERATE_SPLIT, "getSplits error:" + concurrencyPolicy);

        }

        throw new TddlRuntimeException(ErrorCode.ERR_GENERATE_SPLIT, "logicalView is null");

    }

    public static SplitInfo columnarOssTableScanSplit(OSSTableScan ossTableScan, ExecutionContext executionContext,
                                                      long tso) {
        DynamicColumnarManager dynamicColumnarManager = (DynamicColumnarManager) ColumnarManager.getInstance();
        String schemaName = StringUtils.isEmpty(ossTableScan.getSchemaName())
            ? executionContext.getSchemaName() : ossTableScan.getSchemaName();
        String tableName = ossTableScan.getLogicalTableName();

        SortedMap<Long, PartitionInfo> multiVersionPartitionInfo =
            dynamicColumnarManager.getPartitionInfos(tso, schemaName, tableName);

        // Empty partition info demonstrates that the snapshot is empty so far
        if (multiVersionPartitionInfo.isEmpty()) {
            return new SplitInfo(ossTableScan.getRelatedId(), ossTableScan.isExpandView(), FILE_CONCURRENT,
                ImmutableList.of(ImmutableList.of()), new HashMap<>(), 1, 0, false);
        }

        // TODO(siyun): add cache for columnar pruning result
        SortedMap<Long, Set<String>> multiVersionPartitionResult =
            OptimizerUtils.pruningInValueForColumnar(ossTableScan, executionContext, multiVersionPartitionInfo);

        // There is no raw-string pruning, regenerate pruned result
        if (multiVersionPartitionResult == null) {
            multiVersionPartitionResult = new ConcurrentSkipListMap<>();
            final SortedMap<Long, Set<String>> finalResult = multiVersionPartitionResult;
            multiVersionPartitionInfo.forEach((lowerBoundTso, partitionInfo) -> {
                PartPrunedResult tablePrunedResult =
                    ossTableScan.getCciPartPrunedResults(executionContext, partitionInfo).get(0);

                finalResult.put(
                    lowerBoundTso,
                    tablePrunedResult.getPrunedPartitions().stream().map(
                        PhysicalPartitionInfo::getPartName
                    ).collect(Collectors.toSet())
                );
            });
        }

        SqlNode sqlTemplate = ossTableScan.getSqlTemplate(executionContext);
        List<DynamicParamInfo> dynamicParamList = PlannerUtils.getDynamicParamInfoList(sqlTemplate);
        Map<Integer, ParameterContext> params =
            executionContext.getParams() == null ? null : executionContext.getParams().getCurrentParameter();
        Map<Integer, ParameterContext> results = new HashMap<>();

        RelPartitionWise partitionWise = ossTableScan.getTraitSet().getPartitionWise();
        boolean needPartition = partitionWise.isRemotePartition() || executionContext.getParamManager()
            .getBoolean(ConnectionParams.SCHEDULE_BY_PARTITION);

        boolean localPairWise =
            executionContext.getParamManager().getBoolean(ConnectionParams.ENABLE_LOCAL_PARTITION_WISE_JOIN)
                && partitionWise.isLocalPartition();

        if (params != null) {
            for (DynamicParamInfo dynamicParamInfo : dynamicParamList) {
                if (dynamicParamInfo instanceof IndexedDynamicParamInfo) {
                    int i = ((IndexedDynamicParamInfo) dynamicParamInfo).getParamIndex();
                    if (i != PlannerUtils.TABLE_NAME_PARAM_INDEX && i != PlannerUtils.SCALAR_SUBQUERY_PARAM_INDEX
                        && i != PlannerUtils.APPLY_SUBQUERY_PARAM_INDEX) {
                        results.put(i, params.get(i + 1));
                    }
                } else {
                    throw new IllegalArgumentException("Unsupported dynamic param info: " + dynamicParamInfo);
                }
            }
        }

        boolean forceDisableCsvCache =
            !executionContext.getParamManager().getBoolean(ConnectionParams.ENABLE_COLUMNAR_CSV_CACHE);
        final List<Split> splitList = new ArrayList<>();
        boolean autoPosition =
            executionContext.getParamManager().getBoolean(ConnectionParams.ENABLE_COLUMNAR_SNAPSHOT_AUTO_POSITION);
        if (ossTableScan.isFlashbackQuery() || forceDisableCsvCache) {
            FlashbackColumnarManager flashbackColumnarManager =
                dynamicColumnarManager.getFlashbackColumnarManager(tso, schemaName, tableName, autoPosition);
            Map<String, List<Pair<String, Long>>> deletePositionMap =
                flashbackColumnarManager.getDeletePositions(multiVersionPartitionResult);
            Long deltaTso = flashbackColumnarManager.getColumnarDeltaCheckpointTso();
            for (Map.Entry<String, ColumnarPartitionPrunedSnapshot> entry :
                flashbackColumnarManager.getSnapshotInfo(multiVersionPartitionResult).entrySet()) {
                String partName = entry.getKey();
                List<Pair<String, Long>> orcFileNames = entry.getValue().getOrcFilesAndSchemaTs();
                List<Pair<String, Pair<Long, Long>>> csvFileNamesAndPos =
                    entry.getValue().getCsvFilesAndSchemaTsWithPos();

                for (Pair<String, Long> fileAndSchemaTs : orcFileNames) {
                    PartitionInfo currentPartitionInfo = multiVersionPartitionInfo.get(fileAndSchemaTs.getValue());
                    OssSplit.DeltaReadOption deltaReadOption =
                        new OssSplit.DeltaReadOption(deltaTso != null ? deltaTso : tso);
                    deltaReadOption.setAllDelPositions(deletePositionMap);
                    Pair<Integer, Pair<String, String>> partSpecInfo =
                        PartitionUtils.calcPartition(currentPartitionInfo, partName);
                    int partition = needPartition ? partSpecInfo.getKey() : NO_PARTITION_INFO;
                    String phySchema = partSpecInfo.getValue().getKey();
                    String phyTableName = partSpecInfo.getValue().getValue();

                    splitList.add(new Split(false, new OssSplit(
                        schemaName, phySchema, results, tableName, Collections.singletonList(phyTableName),
                        Collections.singletonList(fileAndSchemaTs.getKey()), deltaReadOption, tso, partition,
                        localPairWise
                    )));
                }

                for (Pair<String, Pair<Long, Long>> fileNameAndTsWithPos : csvFileNamesAndPos) {
                    PartitionInfo currentPartitionInfo =
                        multiVersionPartitionInfo.get(fileNameAndTsWithPos.getValue().getKey());
                    OssSplit.DeltaReadOption deltaReadOption =
                        new OssSplit.DeltaReadOption(deltaTso != null ? deltaTso : tso);
                    deltaReadOption.setAllCsvFiles(Collections.singletonMap(partName,
                        Collections.singletonList(fileNameAndTsWithPos.getKey())));
                    deltaReadOption.setAllPositions(Collections.singletonMap(partName,
                        Collections.singletonList(fileNameAndTsWithPos.getValue().getValue())));
                    deltaReadOption.setAllDelPositions(deletePositionMap);

                    Pair<Integer, Pair<String, String>> partSpecInfo =
                        PartitionUtils.calcPartition(currentPartitionInfo, partName);
                    int partition = needPartition ? partSpecInfo.getKey() : NO_PARTITION_INFO;
                    String phySchema = partSpecInfo.getValue().getKey();
                    String phyTableName = partSpecInfo.getValue().getValue();

                    splitList.add(new Split(false, new OssSplit(
                        schemaName, phySchema, results, tableName, Collections.singletonList(phyTableName), null,
                        deltaReadOption, tso, partition, localPairWise
                    )));
                }
            }
        } else {
            for (Map.Entry<String, ColumnarPartitionPrunedSnapshot> entry : dynamicColumnarManager.findFileNames(
                tso, schemaName, tableName, multiVersionPartitionResult).entrySet()) {
                String partName = entry.getKey();
                ColumnarPartitionPrunedSnapshot snapshotInfo = entry.getValue();
                List<Pair<String, Long>> orcFilesAndSchemaTs = snapshotInfo.getOrcFilesAndSchemaTs();
                List<Pair<String, Pair<Long, Long>>> csvFilesAndSchemaTsWithPos =
                    snapshotInfo.getCsvFilesAndSchemaTsWithPos();

                for (Pair<String, Long> fileAndSchemaTs : orcFilesAndSchemaTs) {
                    PartitionInfo currentPartitionInfo = multiVersionPartitionInfo.get(fileAndSchemaTs.getValue());
                    Pair<Integer, Pair<String, String>> partSpecInfo =
                        PartitionUtils.calcPartition(currentPartitionInfo, partName);
                    int partition = needPartition ? partSpecInfo.getKey() : NO_PARTITION_INFO;
                    String phySchema = partSpecInfo.getValue().getKey();
                    String phyTableName = partSpecInfo.getValue().getValue();
                    splitList.add(new Split(false, new OssSplit(
                        schemaName, phySchema, results, tableName, Collections.singletonList(phyTableName),
                        Collections.singletonList(fileAndSchemaTs.getKey()), null, tso, partition, localPairWise
                    )));
                }

                for (Pair<String, Pair<Long, Long>> fileNameAndTsWithPos : csvFilesAndSchemaTsWithPos) {
                    PartitionInfo currentPartitionInfo =
                        multiVersionPartitionInfo.get(fileNameAndTsWithPos.getValue().getKey());

                    Pair<Integer, Pair<String, String>> partSpecInfo =
                        PartitionUtils.calcPartition(currentPartitionInfo, partName);
                    int partition = needPartition ? partSpecInfo.getKey() : NO_PARTITION_INFO;
                    String phySchema = partSpecInfo.getValue().getKey();
                    String phyTableName = partSpecInfo.getValue().getValue();

                    OssSplit.DeltaReadOption deltaReadOption = new OssSplit.DeltaReadOption(tso);
                    deltaReadOption.setAllCsvFiles(
                        Collections.singletonMap(partName, Collections.singletonList(fileNameAndTsWithPos.getKey()))
                    );

                    splitList.add(new Split(false, new OssSplit(
                        schemaName, phySchema, results, tableName, Collections.singletonList(phyTableName), null,
                        deltaReadOption, tso, partition, localPairWise
                    )));
                }
            }
        }

        if (splitList.size() > 0) {
            executionContext.setHasScanWholeTable(true);
        }
        return new SplitInfo(ossTableScan.getRelatedId(), ossTableScan.isExpandView(), FILE_CONCURRENT,
            ImmutableList.of(splitList), new HashMap<>(), 1, splitList.size(), false);
    }

    private SplitInfo ossTableScanSplit(
        OSSTableScan ossTableScan, ExecutionContext executionContext, boolean highConcurrencyQuery) {
        if (ossTableScan == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_GENERATE_SPLIT, "logicalView is null");
        }

        QueryConcurrencyPolicy concurrencyPolicy =
            ExecUtils.getQueryConcurrencyPolicy(executionContext, ossTableScan);

        // Before allocating splits, a certain tso must be fetched first
        Long tso = fetchTsoAndInitTransaction(ossTableScan, executionContext);

        List<RelNode> inputs = ExecUtils.getInputs(
            ossTableScan, executionContext, !ExecUtils.isMppMode(executionContext));

        if (executionContext.isCciIncrementalCheck()
            || executionContext.isReadCsvOnly()
            || executionContext.isReadOrcOnly()
            || executionContext.isReadSpecifiedColumnarFiles()) {
            // Splits are already partitioned by partition.
            // Normal query should not get here.
            return generateSpecifiedSplitInfo(ossTableScan, inputs, executionContext);
        }

        String schemaName = ossTableScan.getSchemaName();
        if (StringUtils.isEmpty(schemaName)) {
            schemaName = executionContext.getSchemaName();
        }

        if (ossTableScan.isColumnarIndex()
            && CBOUtil.isArchiveCCi(schemaName, ossTableScan.getLogicalTableName())
            // In case multi-version partition info is disrupted or too slow
            // SET ENABLE_COLUMNAR_MULTI_VERSION_PARTITION=false to fall back to old logic
            && executionContext.getParamManager()
            .getBoolean(ConnectionParams.ENABLE_COLUMNAR_MULTI_VERSION_PARTITION)) {
            return columnarOssTableScanSplit(ossTableScan, executionContext, tso);
        }

        if (inputs.size() > 1) {
            // record full table scan
            executionContext.setHasScanWholeTable(true);
        }

        HashMap<String, String> shardSet = new HashMap<>();
        int splitCount = 0;

        // if storage is columnar index, zigzag by mysql instance has no meaning
        List<RelNode> sortInputs = ossTableScan.isColumnarIndex() ? inputs :
            ExecUtils.zigzagInputsByMysqlInst(inputs, schemaName, executionContext);

        List<Split> splitList = new ArrayList<>();

        switch (concurrencyPolicy) {
        case SEQUENTIAL:
        case CONCURRENT:
        case FIRST_THEN_CONCURRENT:
        case GROUP_CONCURRENT_BLOCK:
            // split according to physical table operations.
            for (RelNode input : sortInputs) {
                List<OssSplit> splits = OssSplit.getTableConcurrencySplit(ossTableScan, input, executionContext, tso);
                for (OssSplit split : splits) {
                    shardSet.put(split.getPhysicalSchema(), split.getLogicalSchema());
                    splitList.add(new Split(false, split));
                    splitCount++;
                }
            }
            return new SplitInfo(ossTableScan.getRelatedId(), ossTableScan.isExpandView(),
                concurrencyPolicy == FIRST_THEN_CONCURRENT ? GROUP_CONCURRENT_BLOCK : concurrencyPolicy,
                ImmutableList.of(splitList),
                shardSet, 1,
                splitCount, false);
        case FILE_CONCURRENT:
            // split according to all table files.
            for (RelNode input : sortInputs) {
                List<OssSplit> splits = OssSplit.getFileConcurrencySplit(ossTableScan, input, executionContext, tso);
                if (splits != null) {
                    for (OssSplit split : splits) {
                        shardSet.put(split.getPhysicalSchema(), split.getLogicalSchema());
                        splitList.add(new Split(false, split));
                        splitCount++;
                    }
                }
            }
            return new SplitInfo(ossTableScan.getRelatedId(), ossTableScan.isExpandView(),
                FILE_CONCURRENT,
                ImmutableList.of(splitList),
                shardSet, 1,
                splitCount, false);
        case INSTANCE_CONCURRENT:
        default:
            break;
        }
        throw new TddlRuntimeException(ErrorCode.ERR_GENERATE_SPLIT, "getSplits error:" + concurrencyPolicy);

    }

    private JdbcSplit parseRelNode(LogicalView logicalView, TopologyHandler topology, RelNode input,
                                   String schemaName, byte[] hint, ITransaction.RW rw, ExecutionContext ec) {
        if (input instanceof PhyTableOperation) {
            PhyTableOperation phyTableOperation = (PhyTableOperation) input;

            LookupSql lookupSql = generateLookupSql(logicalView, phyTableOperation);

            IGroupExecutor groupExecutor = topology.get(phyTableOperation.getDbIndex());
            TGroupDataSource ds = (TGroupDataSource) groupExecutor.getDataSource();
            String address = LOCAL_ADDRESS;
            if (!DynamicConfig.getInstance().enableExtremePerformance()) {
                address = ds.getOneAtomAddress(ConfigDataMode.isMasterMode());
            }

            PhyTableScanBuilder phyOperationBuilder =
                (PhyTableScanBuilder) phyTableOperation.getPhyOperationBuilder();
            final ByteString galaxyDigestBS = phyTableOperation.getGalaxyPrepareDigest();
            final byte[] galaxyDigest = null == galaxyDigestBS ? null : galaxyDigestBS.toByteArray();

            boolean useParameterDelegate = ExecUtils.useParameterDelegate(ec);
            List<List<ParameterContext>> params = new ArrayList<>(phyTableOperation.getTableNames().size());
            for (List<String> tables : phyTableOperation.getTableNames()) {
                params.add(phyOperationBuilder.buildSplitParams(
                    phyTableOperation.getDbIndex(), tables, useParameterDelegate));
            }

            Long intraGroupSortKey = PhyTableOperationUtil.fetchPhyOpIntraGroupConnKey(phyTableOperation, ec);
            return new JdbcSplit(ds.getDbGroupKey(),
                schemaName,
                phyTableOperation.getDbIndex(),
                hint,
                lookupSql.bytesSql,
                lookupSql.orderBy,
                params,
                address,
                phyTableOperation.getTableNames(),
                rw,
                phyOperationBuilder.containLimit() || (logicalView.getLockMode() != null
                    && logicalView.getLockMode() != SqlSelect.LockMode.UNDEF),
                intraGroupSortKey,
                galaxyDigest, galaxyDigest != null && phyTableOperation.isSupportGalaxyPrepare(),
                lookupSql.selectNode,
                lookupSql.startSql);
        } else {
            throw new UnsupportedOperationException("Unknown input " + input);
        }
    }

    protected LookupSql generateLookupSql(LogicalView logicalView, PhyTableOperation phyTableOperation) {
        LookupSql lookupSql = null;
        if (logicalView.isMGetEnabled()) {
            lookupSql = logicalView.getLookupSqlTemplateCache(() -> {
                return innerGenerateLookupSql(logicalView, phyTableOperation);
            });
            phyTableOperation.setBytesSql(lookupSql.bytesSql);
        } else {
            PhyTableScanBuilder phyOperationBuilder =
                (PhyTableScanBuilder) phyTableOperation.getPhyOperationBuilder();
            String orderBy = phyOperationBuilder.buildPhysicalOrderByClause();
            lookupSql = new LookupSql(phyTableOperation.getBytesSql(), null, orderBy, null);
        }
        return lookupSql;
    }

    private SplitInfo generateSpecifiedSplitInfo(OSSTableScan ossTableScan,
                                                 List<RelNode> sortInputs,
                                                 ExecutionContext ec) {
        HashMap<String, String> shardSet = new HashMap<>();
        List<Split> splitList = new ArrayList<>();
        int splitCount = 0;
        for (RelNode input : sortInputs) {
            List<OssSplit> splits = SpecifiedOssSplit.getSpecifiedSplit(ossTableScan, input, ec);
            for (OssSplit split : splits) {
                shardSet.put(split.getPhysicalSchema(), split.getLogicalSchema());
                splitList.add(new Split(false, split));
                splitCount++;
            }
        }
        return new SplitInfo(ossTableScan.getRelatedId(), ossTableScan.isExpandView(),
            FILE_CONCURRENT,
            ImmutableList.of(splitList),
            shardSet, 1,
            splitCount, false);
    }

    Long fetchTsoAndInitTransaction(OSSTableScan ossTableScan, ExecutionContext executionContext) {
        Long tso = null;
        boolean forceUseLatestTso =
            executionContext.getParamManager().getBoolean(ConnectionParams.USE_LATEST_COLUMNAR_TSO);
        if (ossTableScan.isColumnarIndex()) {
            ITransaction trans = executionContext.getTransaction();
            if (trans instanceof IColumnarTransaction) {
                Long flashBackQueryTso = ossTableScan.getFlashbackQueryTso(executionContext);
                if (flashBackQueryTso != null) {
                    tso = flashBackQueryTso;
                    // flashback query tso may differ among tables,
                    // so we do not set tso for columnar trans
                } else {
                    IColumnarTransaction columnarTrans = (IColumnarTransaction) trans;
                    if (!columnarTrans.snapshotSeqIsEmpty()) {
                        tso = columnarTrans.getSnapshotSeq();
                    } else {
                        if (forceUseLatestTso) {
                            tso = ColumnarTransactionUtils.getLatestTsoFromGms();
                        } else {
                            tso = ColumnarManager.getInstance().latestTso();
                        }
                        columnarTrans.setTsoTimestamp(tso);
                    }
                }
            } else {
                LOGGER.warn("Trying to access columnar index out of IMppReadOnlyTransaction, transaction class is: "
                    + trans.getTransactionClass().name());
                Long flashBackQueryTso = ossTableScan.getFlashbackQueryTso(executionContext);
                if (flashBackQueryTso != null) {
                    tso = flashBackQueryTso;
                } else {
                    if (forceUseLatestTso) {
                        tso = ColumnarTransactionUtils.getLatestTsoFromGms();
                    } else {
                        tso = ColumnarManager.getInstance().latestTso();
                    }
                }
            }
        }
        return tso;
    }

    protected LookupSql innerGenerateLookupSql(LogicalView logicalView, PhyTableOperation phyTableOperation) {
        if (logicalView.isInToUnionAll()) {
            SqlSelect nativeSqlForMget = (SqlSelect) ((SqlSelect) phyTableOperation.getNativeSqlNode());
            SqlNodeList selectList = nativeSqlForMget.getSelectList();
            boolean existAs = false;
            if (selectList != null) {
                for (SqlNode selectNode : selectList) {
                    if (selectNode.getKind() == SqlKind.AS) {
                        existAs = true;
                        break;
                    }
                }
            }
            PhyTableScanBuilder phyOperationBuilder =
                (PhyTableScanBuilder) phyTableOperation.getPhyOperationBuilder();
            String orderBy = phyOperationBuilder.buildPhysicalOrderByClause();
            if (existAs) {
                BytesSql bytesSql = RelUtils.toNativeBytesSql(nativeSqlForMget);
                return new LookupSql(bytesSql, bytesSql, orderBy, null);
            } else {
                nativeSqlForMget = (SqlSelect) ((SqlSelect) nativeSqlForMget.clone(
                    phyTableOperation.getNativeSqlNode().getParserPosition()));
                nativeSqlForMget.setComputedFetch(
                    ((SqlSelect) phyTableOperation.getNativeSqlNode()).getComputedFetch());
                final List<SqlNode> startSelectItems = new ArrayList<>();
                startSelectItems.add(SqlIdentifier.star(SqlParserPos.ZERO));
                nativeSqlForMget.setSelectList(new SqlNodeList(startSelectItems, SqlParserPos.ZERO));
                BytesSql startSql = RelUtils.toNativeBytesSql(nativeSqlForMget);
                return new LookupSql(phyTableOperation.getBytesSql(), startSql, orderBy,
                    selectList.toString());
            }
        } else {
            SqlSelect nativeSqlForMget = (SqlSelect) ((SqlSelect) phyTableOperation.getNativeSqlNode())
                .clone(phyTableOperation.getNativeSqlNode().getParserPosition());
            nativeSqlForMget.setComputedFetch(
                ((SqlSelect) phyTableOperation.getNativeSqlNode()).getComputedFetch());
            SqlNode filter = nativeSqlForMget.getWhere();
            SqlNode customFilter;
            if (filter != null) {
                SqlOperator operator = SqlStdOperatorTable.AND;
                customFilter = new SqlBasicCall(operator,
                    new SqlNode[] {filter, DYNAMIC_CONDITION_PLACEHOLDER},
                    SqlParserPos.ZERO);
            } else {
                customFilter = DYNAMIC_CONDITION_PLACEHOLDER;
            }
            nativeSqlForMget.setWhere(customFilter);
            BytesSql bytesSql = RelUtils.toNativeBytesSql(nativeSqlForMget);
            PhyTableScanBuilder phyOperationBuilder =
                (PhyTableScanBuilder) phyTableOperation.getPhyOperationBuilder();
            String orderBy = phyOperationBuilder.buildPhysicalOrderByClause();
            return new LookupSql(bytesSql, null, orderBy, null);
        }
    }
}
