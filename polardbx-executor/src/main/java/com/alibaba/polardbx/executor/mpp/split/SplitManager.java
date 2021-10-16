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

import com.alibaba.polardbx.atom.TAtomDataSource;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.common.TopologyHandler;
import com.alibaba.polardbx.executor.mpp.metadata.Split;
import com.alibaba.polardbx.executor.spi.IGroupExecutor;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.executor.utils.SubqueryUtils;
import com.alibaba.polardbx.group.jdbc.DataSourceWrapper;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.BaseQueryOperation;
import com.alibaba.polardbx.optimizer.core.rel.BaseTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableScanBuilder;
import com.alibaba.polardbx.optimizer.utils.ITransaction;
import com.alibaba.polardbx.optimizer.utils.QueryConcurrencyPolicy;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.lang.StringUtils;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.alibaba.polardbx.common.model.SqlType.SELECT_FOR_UPDATE;
import static com.alibaba.polardbx.executor.utils.ExecUtils.buildDRDSTraceComment;
import static com.alibaba.polardbx.optimizer.utils.QueryConcurrencyPolicy.CONCURRENT;
import static com.alibaba.polardbx.optimizer.utils.QueryConcurrencyPolicy.GROUP_CONCURRENT_BLOCK;

public class SplitManager {

    private static final Logger logger = LoggerFactory.getLogger(SplitManager.class);

    public static SqlNode DYNAMIC_CONDITION_PLACEHOLDER = new SqlBasicCall(SqlStdOperatorTable.EQUALS,
        new SqlNode[] {
            SqlLiteral.createCharString("bka_magic", SqlParserPos.ZERO),
            SqlLiteral.createCharString("bka_magic", SqlParserPos.ZERO)}, SqlParserPos.ZERO);

    public SplitInfo getSingleSplit(LogicalView logicalView, ExecutionContext executionContext) {
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
        } else if (executionContext.getSqlType() == SELECT_FOR_UPDATE) {
            rw = ITransaction.RW.WRITE;
        }

        if (queryOperation instanceof PhyTableOperation) {
            PhyTableScanBuilder phyOperationBuilder =
                (PhyTableScanBuilder) ((PhyTableOperation) queryOperation).getPhyOperationBuilder();
            if (phyOperationBuilder != null) {
                List<List<String>> groupTables = ((PhyTableOperation) queryOperation).getTableNames();
                params = new ArrayList<>(groupTables.size());
                for (List<String> tables : groupTables) {
                    params.add(phyOperationBuilder.buildSplitParams(tables));
                }
                dbIndex = queryOperation.getDbIndex();
            }
        }
        if (params == null) {
            Pair<String, Map<Integer, ParameterContext>> dbIndexAndParam =
                queryOperation
                    .getDbIndexAndParam(executionContext.getParams() == null ? null : executionContext.getParams()
                        .getCurrentParameter(), executionContext);
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
        TAtomDataSource atomDataSource = getAtomDatasource(ds.getAtomDataSources().get(0));
        String hint = buildDRDSTraceComment(executionContext);
        String sqlTemplate = queryOperation.getNativeSql();
        JdbcSplit split = new JdbcSplit(ds.getDbGroupKey(),
            schemaName,
            dbIndex,
            hint,
            sqlTemplate,
            null,
            params,
            atomDataSource.getHost() + ":" + atomDataSource.getPort(),
            ImmutableList.of(logicalView.getTableNames()),
            rw,
            false);
        splitList.add(new Split(false, split));

        HashMap<String, String> groups = new HashMap<>();
        groups.put(dbIndex, schemaName);
        return new SplitInfo(logicalView.getRelatedId(), false, QueryConcurrencyPolicy.SEQUENTIAL,
            ImmutableList.of(splitList), groups, 1,
            1, false);
    }

    public SplitInfo getSplits(
        LogicalView logicalView, ExecutionContext executionContext, boolean highConcurrencyQuery) {
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

            String hint = buildDRDSTraceComment(executionContext);

            String schemaName = logicalView.getSchemaName();
            if (StringUtils.isEmpty(schemaName)) {
                schemaName = executionContext.getSchemaName();
            }

            TopologyHandler topology = ExecutorContext.getContext(logicalView.getSchemaName())
                .getTopologyHandler();

            HashMap<String, String> shardSet = new HashMap<>();
            Set<String> instSet = new HashSet<>();
            int splitCount = 0;
            switch (concurrencyPolicy) {
            case SEQUENTIAL:
            case CONCURRENT:
                List<RelNode> sortInputs = ExecUtils.zigzagInputsByMysqlInst(inputs, schemaName);
                List<Split> splitList = new ArrayList<>();
                for (RelNode input : sortInputs) {
                    JdbcSplit split =
                        this.parseRelNode(logicalView, topology, input, logicalView.getSchemaName(), hint, rw);
                    if (split != null) {
                        shardSet.put(split.getDbIndex(), split.getSchemaName());
                        instSet.add(split.getHostAddress());
                        splitList.add(new Split(false, split));
                        splitCount++;
                    }
                }
                return new SplitInfo(logicalView.getRelatedId(), logicalView.isExpandView(), concurrencyPolicy,
                    ImmutableList.of(splitList),
                    shardSet, instSet.size(),
                    splitCount, underSort);
            case FIRST_THEN_CONCURRENT: //只有对广播表写的时候才会用到，在查询的时候我们恢复默认策略即可GROUP_CONCURRENT_BLOCK
            case GROUP_CONCURRENT_BLOCK:
                if (!allowMultipleReadConns) {
                    Map<String, List<Split>> splitAssignment = new HashMap<>();
                    Map<String, List<String>> instDbMap = new LinkedHashMap<>();
                    List<List<Split>> outList = new ArrayList<>();
                    int maxInstDbsize = 0;
                    for (RelNode input : inputs) {
                        JdbcSplit split =
                            this.parseRelNode(logicalView, topology, input, logicalView.getSchemaName(), hint, rw);
                        if (split != null) {
                            shardSet.put(split.getDbIndex(), split.getSchemaName());
                            instSet.add(split.getHostAddress());
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
                        outList, shardSet,
                        instSet.size(),
                        splitCount, underSort);
                } else {
                    List<RelNode> sortInputByInts = ExecUtils.zigzagInputsByMysqlInst(inputs, schemaName);
                    List<Split> retLists = new ArrayList<>();
                    for (RelNode input : sortInputByInts) {
                        JdbcSplit split =
                            this.parseRelNode(logicalView, topology, input, logicalView.getSchemaName(), hint, rw);
                        if (split != null) {
                            shardSet.put(split.getDbIndex(), split.getSchemaName());
                            instSet.add(split.getHostAddress());
                            retLists.add(new Split(false, split));
                            splitCount++;
                        }
                    }
                    return new SplitInfo(logicalView.getRelatedId(), logicalView.isExpandView(), GROUP_CONCURRENT_BLOCK,
                        ImmutableList.of(retLists),
                        shardSet,
                        instSet.size(), splitCount, underSort);
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

    private JdbcSplit parseRelNode(LogicalView logicalView, TopologyHandler topology, RelNode input,
                                   String schemaName, String hint, ITransaction.RW rw) {
        if (input instanceof PhyTableOperation) {
            PhyTableOperation phyTableOperation = (PhyTableOperation) input;

            if (logicalView.isMGetEnabled()) {
                phyTableOperation.setSqlTemplate(logicalView.getLookupSqlTemplateCache(() -> {
                    SqlSelect nativeSqlForMget = (SqlSelect) phyTableOperation.getNativeSqlNode()
                        .clone(phyTableOperation.getNativeSqlNode().getParserPosition());
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
                    return RelUtils.toNativeSql(nativeSqlForMget);
                }));
            }

            IGroupExecutor groupExecutor = topology.get(phyTableOperation.getDbIndex());
            TGroupDataSource ds = (TGroupDataSource) groupExecutor.getDataSource();
            TAtomDataSource atomDataSource = getAtomDatasource(ds.getAtomDataSources().get(0));

            PhyTableScanBuilder phyOperationBuilder =
                (PhyTableScanBuilder) phyTableOperation.getPhyOperationBuilder();
            String orderBy = phyOperationBuilder.buildPhysicalOrderByClause();
            String sqlTemplate = phyTableOperation.getNativeSql();

            List<List<ParameterContext>> params = new ArrayList<>(phyTableOperation.getTableNames().size());
            for (List<String> tables : phyTableOperation.getTableNames()) {
                params.add(phyOperationBuilder.buildSplitParams(tables));
            }
            return new JdbcSplit(ds.getDbGroupKey(),
                schemaName,
                phyTableOperation.getDbIndex(),
                hint,
                sqlTemplate,
                orderBy,
                params,
                atomDataSource.getHost() + ":" + atomDataSource.getPort(),
                phyTableOperation.getTableNames(),
                rw,
                phyOperationBuilder.containLimit() || (logicalView.getLockMode() != null
                    && logicalView.getLockMode() != SqlSelect.LockMode.UNDEF));
        } else {
            throw new UnsupportedOperationException("Unknown input " + input);
        }
    }

    private TAtomDataSource getAtomDatasource(DataSource s) {
        if (s instanceof TAtomDataSource) {
            return (TAtomDataSource) s;
        }
        if (s instanceof DataSourceWrapper) {
            return getAtomDatasource(((DataSourceWrapper) s).getWrappedDataSource());
        }
        throw new IllegalAccessError();
    }
}
