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

package com.alibaba.polardbx.optimizer.core.rel;

import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.BytesSql;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.jdbc.RawString;
import com.alibaba.polardbx.common.model.sqljep.Comparative;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.meta.CostModelWeight;
import com.alibaba.polardbx.optimizer.config.meta.DrdsRelOptCostImpl;
import com.alibaba.polardbx.optimizer.config.meta.TableScanIOEstimator;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.Xplan.XPlanTemplate;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.dialect.DbType;
import com.alibaba.polardbx.optimizer.core.join.EquiJoinUtils;
import com.alibaba.polardbx.optimizer.core.join.LookupEquiJoinKey;
import com.alibaba.polardbx.optimizer.core.join.LookupPredicate;
import com.alibaba.polardbx.optimizer.core.join.LookupPredicateBuilder;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.core.planner.SqlConverter;
import com.alibaba.polardbx.optimizer.core.planner.Xplanner.RelToXPlanConverter;
import com.alibaba.polardbx.optimizer.core.planner.Xplanner.RelXPlanOptimizer;
import com.alibaba.polardbx.optimizer.core.planner.Xplanner.SpecialFunctionRelFinder;
import com.alibaba.polardbx.optimizer.core.planner.rule.FilterMergeRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.JoinConditionSimplifyRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.SubQueryToSemiJoinRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.TddlFilterJoinRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.index.Index;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.partition.common.PartKeyLevel;
import com.alibaba.polardbx.optimizer.partition.common.PartitionTableType;
import com.alibaba.polardbx.optimizer.partition.pruning.PartPrunedResult;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPruneStep;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPruner;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPrunerUtils;
import com.alibaba.polardbx.optimizer.partition.pruning.PhysicalPartitionInfo;
import com.alibaba.polardbx.optimizer.planmanager.DRDSRelJsonReader;
import com.alibaba.polardbx.optimizer.rel.rel2sql.TddlRelToSqlConverter;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.sharding.DataNodeChooser;
import com.alibaba.polardbx.optimizer.sharding.result.ExtractionResult;
import com.alibaba.polardbx.optimizer.sharding.result.RelShardInfo;
import com.alibaba.polardbx.optimizer.utils.ExplainUtils;
import com.alibaba.polardbx.optimizer.utils.OptimizerUtils;
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.utils.RexLiteralTypeUtils;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import com.alibaba.polardbx.optimizer.utils.TableTopologyUtil;
import com.alibaba.polardbx.rule.TableRule;
import com.alibaba.polardbx.rule.model.TargetDB;
import com.alibaba.polardbx.rule.utils.CalcParamsAttribute;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import com.googlecode.protobuf.format.JsonFormat;
import com.mysql.cj.x.protobuf.PolarxExecPlan;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.externalize.RelDrdsJsonWriter;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSemiJoin;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexTableInputRef;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.runtime.PredicateImpl;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSelect.LockMode;
import org.apache.calcite.sql.fun.SqlRuntimeFilterFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Util;
import org.apache.commons.lang.StringUtils;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_TABLE_EMPTY_WITH_HINT;

/**
 * @author lingce.ldm 2017-07-07 15:01
 */
public class LogicalView extends TableScan {
    public static final Predicate<LogicalView> IS_SINGLE_GROUP = new PredicateImpl<LogicalView>() {
        @Override
        public boolean test(LogicalView logicalView) {
            return logicalView.isSingleGroup();
        }
    };
    public static final Predicate<LogicalView> NOT_SINGLE_GROUP = new PredicateImpl<LogicalView>() {
        @Override
        public boolean test(LogicalView logicalView) {
            return !logicalView.isSingleGroup();
        }
    };

    private static final Logger logger = LoggerFactory.getLogger(LogicalView.class);
    protected final DbType dbType;
    protected List<String> tableNames = new ArrayList<>();
    protected PushDownOpt pushDownOpt;
    protected boolean finishShard = false;
    protected LockMode lockMode = LockMode.UNDEF;
    /**
     * add cache so that HINT can interpose the state of LogicalView
     */
    protected SqlNode sqlTemplateHintCache;
    protected Map<String, List<List<String>>> targetTablesHintCache;
    protected Map<String, Map<String, Comparative>> comparativeHintCache;
    protected Map<String, PartitionPruneStep> pruneStepHintCache;
    protected boolean crossSingleTable = false;
    protected boolean isMGetEnabled = false;
    protected Join join;
    protected String schemaName;
    protected List<RexDynamicParam> scalarList = Lists.newArrayList();
    protected List<RexFieldAccess> correlateVariableScalar = Lists.newArrayList();
    private volatile RelNode optimizedPushedRelNodeForMetaQueryCache = null;
    private volatile RelNode lastPushedRelNodeCache = null;
    private volatile RelNode mysqlNodeCache = null;

    private boolean expandView = false;
    protected boolean newPartDbTbl = false;
    private BaseQueryOperation queryOperation;
    private boolean fromMergeIndex = false;

    /**
     * For X plan.
     */
    private RelNode XPlanRel = null;
    private XPlanTemplate XPlan = null;

    /**
     * For galaxy prepare.
     */
    private RelNode galaxyPrepareRel = null;
    private boolean supportGalaxyPrepare = false;
    private BytesSql galaxyPrepareSqlTemplateCache = null;
    private ByteString galaxyPrepareDigest = null;

    // Physical query cache
    private SqlNode sqlTemplateCache;
    private String sqlTemplateStringCache;
    private BytesSql lookupSqlTemplateCache; // MPP executor use this to run lookup join
    private ExtractionResult partitionConditionCache;

    private BytesSql bytesSql;

    private RelOptCost selfCost;
    private List<String> columnOrigins = Lists.newArrayList();

    /**
     * for json serialization
     */
    public LogicalView(RelInput relInput) {
        super(relInput.getCluster(),
            relInput.getTraitSet(),
            SqlConverter.getInstance(relInput.getString("schemaName"),
                    PlannerContext.getPlannerContext(relInput.getCluster()).getExecutionContext()).getCatalog()
                .getTableForMember(relInput.getStringList("table")));
        this.tableNames = relInput.getStringList("tableNames");
        this.dbType = DbType.MYSQL;
        this.schemaName = relInput.getString("schemaName");
        this.newPartDbTbl = checkIfNewPartDbTbl(this.tableNames);
        this.partitions = extractPartitionsFromStrList(relInput.getStringList("partitions"));
        this.flashback = relInput.getExpression("flashback");
        isMGetEnabled = relInput.getBoolean("isMGetEnabled", false);
        Map<String, Object> pushDownOptParams = (Map) relInput.get("pushDownOpt");
        DRDSRelJsonReader drdsRelJsonReader = new DRDSRelJsonReader(relInput.getCluster(),
            SqlConverter
                .getInstance(schemaName, PlannerContext.getPlannerContext(relInput.getCluster()).getExecutionContext())
                .getCatalog(),
            null, relInput.supportMpp());
        try {
            RelNode pushRel = drdsRelJsonReader.read((List<Map<String, Object>>) pushDownOptParams.get("pushrels"));

            pushDownOpt = new PushDownOpt(this, pushRel, getDbType(),
                PlannerContext.getPlannerContext(relInput.getCluster()).getExecutionContext());
            this.rebuildPartRoutingPlanInfo();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("PLAN EXTERNALIZE TEST error:" + e.getMessage());
        }
        buildApply();
        rebuildOriginColumnNames();
    }

    public LogicalView(TableScan scan, LockMode lockMode) {
        super(scan.getCluster(), scan.getTraitSet(), scan.getTable(), scan.getHints(), scan.getIndexNode(),
            scan.getFlashback(), scan.getFlashbackOperator(), scan.getPartitions());
        this.dbType = DbType.MYSQL;
        this.tableNames.add(Util.last(table.getQualifiedName()));
        this.schemaName = table.getQualifiedName().size() == 2 ? table.getQualifiedName().get(0) :
            PlannerContext.getPlannerContext(scan).getSchemaName();
        this.newPartDbTbl = checkIfNewPartDbTbl(this.tableNames);
        this.pushDownOpt = new PushDownOpt(this, dbType, PlannerContext.getPlannerContext(scan).getExecutionContext());
        this.lockMode = lockMode;
    }

    public LogicalView(LogicalView newLogicalView) {
        this(newLogicalView, (RelTraitSet) null);
    }

    public LogicalView(LogicalView newLogicalView, RelTraitSet traitSet) {
        super(newLogicalView.getCluster(),
            newLogicalView.getTraitSet(),
            newLogicalView.getTable(),
            newLogicalView.getHints(),
            newLogicalView.getIndexNode(),
            newLogicalView.getFlashback(),
            newLogicalView.getFlashbackOperator(),
            newLogicalView.getPartitions());
        this.dbType = newLogicalView.getDbType();
        this.tableNames.addAll(newLogicalView.getTableNames());
        this.schemaName = newLogicalView.getSchemaName();
        this.newPartDbTbl = checkIfNewPartDbTbl(this.tableNames);
        this.pushDownOpt = newLogicalView.getPushDownOpt();
        this.finishShard = newLogicalView.getFinishShard();
        this.lockMode = newLogicalView.lockMode;
        this.sqlTemplateHintCache = newLogicalView.sqlTemplateHintCache;
        this.targetTablesHintCache = newLogicalView.targetTablesHintCache;
        this.comparativeHintCache = newLogicalView.comparativeHintCache;
        this.crossSingleTable = newLogicalView.crossSingleTable;
        this.isMGetEnabled = newLogicalView.isMGetEnabled;
        this.join = newLogicalView.join;
        this.scalarList = newLogicalView.scalarList;
        this.correlateVariableScalar = newLogicalView.correlateVariableScalar;
        if (traitSet == null) {
            this.traitSet = this.traitSet.replace(Convention.NONE);
        } else {
            this.traitSet = traitSet;
        }
        this.fromMergeIndex = newLogicalView.fromMergeIndex;
        this.columnOrigins = newLogicalView.columnOrigins;
    }

    /**
     * For HINT use only!
     */
    public LogicalView(RelNode rel, RelOptTable table, SqlNodeList hints) {
        this(rel, table, hints, LockMode.UNDEF, null);
    }

    public LogicalView(RelNode rel, RelOptTable table, SqlNodeList hints, LockMode lockMode, SqlNode indexNode) {
        super(rel.getCluster(), rel.getTraitSet(), table, hints, indexNode);
        this.dbType = DbType.MYSQL;
        if (null != table) {
            this.tableNames.add(Util.last(table.getQualifiedName()));
            this.schemaName = table.getQualifiedName().size() == 2 ? table.getQualifiedName().get(0) :
                PlannerContext.getPlannerContext(rel).getSchemaName();
        }
        this.newPartDbTbl = checkIfNewPartDbTbl(this.tableNames);
        this.pushDownOpt =
            new PushDownOpt(this, rel, this.dbType, PlannerContext.getPlannerContext(rel).getExecutionContext());
        this.lockMode = lockMode;
    }

    public static LogicalView create(RelNode rel, RelOptTable table) {
        LogicalView logicalView = new LogicalView(rel, table, null);
        return logicalView;
    }

    public static LogicalView.ReplacedTableCondition getParallelRelNode(RelNode rootRel, List<Object> objects) {
        final LogicalView.DynamicRexReplacer dynamicRexReplacer = new LogicalView.DynamicRexReplacer(objects,
            rootRel.getCluster().getRexBuilder());
        class RelDynamicParamReplacer extends RelShuttleImpl {
            @Override
            public RelNode visit(LogicalFilter filter) {
                RelNode input = filter.getInput();
                if (input instanceof Gather) {
                    input = ((Gather) input).getInput();
                }
                RelNode visit = input;
                if (!(input instanceof TableScan)) {
                    visit = visit(input);
                }
                //assert input instanceof TableScan;

                final RexNode condition = filter.getCondition();
                final RexNode accept = condition.accept(dynamicRexReplacer);
                final LogicalFilter copy = filter.copy(filter.getTraitSet(), visit, accept);
                return copy;

            }
        }
        final RelDynamicParamReplacer relDynamicParamReplacer = new RelDynamicParamReplacer();
        RelNode visit;
        if (rootRel instanceof LogicalFilter) {
            visit = relDynamicParamReplacer.visit((LogicalFilter) rootRel);
        } else {
            visit = relDynamicParamReplacer.visit(rootRel);
        }
        return new LogicalView.ReplacedTableCondition(visit, dynamicRexReplacer.isReplaced());
    }

    private boolean isCacheForbidden() {
        if (scalarList != null &&
            scalarList.size() > 0) {
            return true;
        }

        if (correlateVariableScalar != null &&
            correlateVariableScalar.size() > 0) {
            return true;
        }
        return false;
    }

    @Override
    public void collectVariablesUsed(Set<CorrelationId> variableSet) {
        this.correlateVariableScalar.stream().forEach(
            rexFieldAccess -> variableSet.add(((RexCorrelVariable) rexFieldAccess.getReferenceExpr()).getId()));
    }

    private static RexLiteral makeLiteral(RexBuilder rexBuilder, RelDataType type, Object obj) {
        if (obj == null) {
            return null;
        }
        final DataType dataTypeOfRelTo = DataTypeUtil.calciteToDrdsType(type);
        final DataType typeOfObjectTo = DataTypeUtil.getTypeOfObject(obj);
        if (dataTypeOfRelTo != typeOfObjectTo) {
            return null;
        }
        final Object toObj = typeOfObjectTo.convertFrom(obj);
        final RexLiteral rexLiteral = RexLiteralTypeUtils.convertJavaObjectToRexLiteral(toObj, type,
            type.getSqlTypeName(), rexBuilder);
        return rexLiteral;
    }

    private void buildApply() {
        class ApplyBuilder extends RelVisitor {

            @Override
            public void visit(RelNode relNode, int ordinal, RelNode parent) {
                if (relNode instanceof LogicalProject) {
                    RexUtil.DynamicFinder dynamicFinder = new RexUtil.DynamicFinder();
                    for (RexNode rexNode : ((LogicalProject) relNode).getProjects()) {
                        rexNode.accept(dynamicFinder);
                    }
                    scalarList.addAll(dynamicFinder.getScalar());
                    correlateVariableScalar.addAll(dynamicFinder.getCorrelateVariableScalar());
                } else if (relNode instanceof LogicalFilter) {
                    RexUtil.DynamicFinder dynamicFinder = new RexUtil.DynamicFinder();
                    ((LogicalFilter) relNode).getCondition().accept(dynamicFinder);
                    scalarList.addAll(dynamicFinder.getScalar());
                    correlateVariableScalar.addAll(dynamicFinder.getCorrelateVariableScalar());
                }
                super.visit(relNode, ordinal, parent);
            }

            void run(RelNode node) {
                go(node);
            }
        }
        new ApplyBuilder().run(getPushedRelNode());
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return pw
            .item("table", table.getQualifiedName())
            .item("tableNames", tableNames)
            .item("pushDownOpt", pushDownOpt)
            .item("schemaName", schemaName)
            .itemIf("isMGetEnabled", isMGetEnabled, isMGetEnabled)
            .item("partitions", convertPartitionsToStrList())
            .item("flashback", flashback)
            ;
    }

    private SqlNode extractPartitionsFromStrList(List<String> partNameList) {
        if (partNameList == null || partNameList.size() == 0) {
            return null;
        }

        List<SqlNode> nodeList = new ArrayList<>();
        for (int i = 0; i < partNameList.size(); i++) {
            String partNameStr = partNameList.get(i);
            List<String> tmpVal = new ArrayList<>();
            tmpVal.add(partNameStr);
            SqlIdentifier partNameId = new SqlIdentifier(tmpVal, SqlParserPos.ZERO);
            nodeList.add(partNameId);
        }
        SqlNodeList partNameIds = new SqlNodeList(nodeList, SqlParserPos.ZERO);
        return partNameIds;
    }

    private List<String> convertPartitionsToStrList() {
        List<String> strList = new ArrayList<>();
        if (partitions == null) {
            return strList;
        }
        try {
            SqlNodeList partNameIds = (SqlNodeList) this.partitions;
            for (int i = 0; i < partNameIds.size(); i++) {
                SqlIdentifier partNameId = (SqlIdentifier) partNameIds.get(i);
                strList.add(partNameId.toString());
            }
        } catch (Throwable ex) {
            // ignore
        }
        return strList;
    }

    /**
     * 构建并获取其下层的 PhyTableOperation 节点
     * <p>
     * <pre>
     *     计算分片
     *     构建对应的 PhyTableOperation
     * </pre>
     */
    public List<RelNode> getInput(ExecutionContext executionContext) {
        Map<String, List<List<String>>> targetTables = getTargetTables(executionContext);
        SqlSelect sqlTemplate = (SqlSelect) getSqlTemplate(executionContext);

        PhyTableScanBuilder phyTableScanbuilder = new PhyTableScanBuilder(sqlTemplate,
            targetTables,
            executionContext,
            this,
            dbType,
            schemaName,
            tableNames);
        return phyTableScanbuilder.build(executionContext);
    }

    public List<RelNode> getInputWithoutCache(ExecutionContext executionContext) {
        Map<String, List<List<String>>> targetTables = getTargetTables(executionContext);
        SqlSelect sqlTemplate = (SqlSelect) getSqlTemplate(executionContext);

        PhyTableScanBuilder phyTableScanbuilder = new PhyTableScanBuilder(sqlTemplate,
            targetTables,
            executionContext,
            this,
            dbType,
            schemaName,
            tableNames,
            false);
        return phyTableScanbuilder.build(executionContext);
    }

    public List<RelNode> getInnerInput(UnionOptHelper helper, ExecutionContext executionContext,
                                       boolean forceIgnoreRF) {
        SqlSelect sqlTemplate = (SqlSelect) getSqlTemplate(executionContext);

        return getInnerInput(sqlTemplate, helper, executionContext, forceIgnoreRF);
    }

    public List<RelNode> getInnerInput(SqlSelect sqlTemplate, UnionOptHelper helper,
                                       ExecutionContext executionContext) {
        return getInnerInput(sqlTemplate, helper, executionContext, false);
    }

    private List<RelNode> getInnerInput(SqlSelect sqlTemplate, UnionOptHelper helper,
                                        ExecutionContext executionContext, boolean forceIgnoreRF) {
        Map<String, List<List<String>>> targetTables;
        Map<com.alibaba.polardbx.common.utils.Pair<String, List<String>>, Parameters> pruningMap =
            OptimizerUtils.pruningInValue(this, executionContext);
        if (pruningMap == null) {
            targetTables = getTargetTables(executionContext);
        } else {
            targetTables = transformToTargetTables(pruningMap);
        }
        if (forceIgnoreRF) {
            sqlTemplate = (SqlSelect) sqlTemplate.accept(new SqlShuttle() {
                @Override
                public SqlNode visit(SqlCall call) {
                    SqlKind kind = call.getKind();
                    if (kind == SqlKind.RUNTIME_FILTER) {
                        return SqlLiteral.createBoolean(true, SqlParserPos.ZERO);
                    }
                    return super.visit(call);
                }
            });
        }

        PhyTableScanBuilder phyTableScanbuilder = new PhyTableScanBuilder(sqlTemplate,
            targetTables,
            executionContext,
            this,
            dbType,
            schemaName,
            tableNames,
            pruningMap
        );
        phyTableScanbuilder.setUnionOptHelper(helper);
        return phyTableScanbuilder.build(executionContext);
    }

    public Map<String, List<List<String>>> getTargetTables(ExecutionContext executionContext) {
        if (null == this.targetTablesHintCache) {
            return buildTargetTables(executionContext);
        } else {
            return this.targetTablesHintCache;
        }
    }

    public Map<String, List<List<String>>> buildTargetTables(ExecutionContext executionContext) {
        if (!newPartDbTbl) {
            return buildTargetTablesForShardDbTb(executionContext);
        } else {
            return buildTargetTablesForPartitionTb(executionContext);
        }
    }

    protected Map<String, List<List<String>>> transformToTargetTables(
        Map<com.alibaba.polardbx.common.utils.Pair<String, List<String>>, Parameters> prunedParameters) {
        Map<String, List<List<String>>> targetTables = new HashMap<>();
        for (com.alibaba.polardbx.common.utils.Pair<String, List<String>> pair : prunedParameters.keySet()) {
            String group = pair.getKey();
            List<String> tableNames = pair.getValue();
            List<List<String>> tablesList = targetTables.get(group);
            if (tablesList == null) {
                tablesList = new ArrayList<>();
                targetTables.put(group, tablesList);
            }
            tablesList.add(tableNames);
        }
        return targetTables;
    }

    protected Map<String, List<List<String>>> buildTargetTablesForPartitionTb(ExecutionContext executionContext) {
        final List<PartPrunedResult> resultList = getPartPrunedResults(executionContext);
        Map<String, List<List<String>>> rs = PartitionPrunerUtils.buildTargetTablesByPartPrunedResults(resultList);
        return rs;
    }

    @NotNull
    public List<PartPrunedResult> getPartPrunedResults(ExecutionContext executionContext) {
        List<PartPrunedResult> resultList = PartitionPruner.prunePartitions(this, executionContext);
        filterPrunedResultBySelectedPartitions(resultList);
        return resultList;
    }

    private void validateSelectedPartitions(boolean isNewPartDb, PartitionInfo partInfo, boolean isJoin) {
        if (this.partitions != null) {
            if (!isNewPartDb) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, "Do not support table with mysql partition.");
            } else {
                boolean singleTbl = partInfo.isSingleTable();
                boolean broadcastTbl = partInfo.isBroadcastTable();
                if (singleTbl || (!isJoin && broadcastTbl)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                        "PARTITION () clause on non partitioned table");
                }
            }

        }
    }

    private void buildPrunedResultByTargetTablesHintCache(List<PartPrunedResult> resultList) {
        Map<String, List<List<String>>> targetTbls = this.targetTablesHintCache;
        int logTbNum = this.tableNames.size();
        List<PartPrunedResult> prunedResults = new ArrayList<>();
        for (int t = 0; t < logTbNum; t++) {

            String logTb = this.tableNames.get(t);
            PartitionInfo partInfo = OptimizerContext.getContext(schemaName).getPartitionInfoManager()
                .getPartitionInfo(logTb);
            BitSet partBitSet = PartitionPrunerUtils.buildEmptyPhysicalPartitionsBitSet(partInfo);
            for (Map.Entry<String, List<List<String>>> targetTblIem : targetTbls.entrySet()) {
                String grp = targetTblIem.getKey();
                List<List<String>> phyTblsList = targetTblIem.getValue();
                for (int i = 0; i < phyTblsList.size(); i++) {
                    List<String> phyTbsInPartGrp = phyTblsList.get(i);
                    String phyTb = phyTbsInPartGrp.get(t);
                    PartitionSpec pSpec = partInfo.getPartSpecSearcher().getPartSpec(grp, phyTb);
                    if (pSpec != null) {
                        partBitSet.set(pSpec.getPhyPartPosition().intValue() - 1);
                    }
                }
            }
            PartPrunedResult prunedResult =
                PartPrunedResult.buildPartPrunedResult(partInfo, partBitSet, PartKeyLevel.PARTITION_KEY, null, false);
            prunedResults.add(prunedResult);
        }

        if (resultList != null) {
            resultList.addAll(prunedResults);
        }

    }

    protected void filterPrunedResultBySelectedPartitions(List<PartPrunedResult> resultList) {
        if (this.partitions != null) {
            for (int i = 0; i < resultList.size(); i++) {
                PartitionInfo partInfo = resultList.get(i).getPartInfo();
                if (partInfo.getTableType() == PartitionTableType.PARTITION_TABLE
                    || partInfo.getTableType() == PartitionTableType.GSI_TABLE
                    || partInfo.getTableType() == PartitionTableType.COLUMNAR_TABLE) {
                    SqlNodeList partNamesAst = (SqlNodeList) this.partitions;
                    Set<Integer> selectedPartPostSet = new HashSet<>();
                    for (SqlNode partNameAst : partNamesAst.getList()) {
                        String partName = ((SqlIdentifier) partNameAst).getLastName();
                        PartitionSpec pSpec = partInfo.getPartSpecSearcher().getPartSpecByPartName(partName);
                        if (pSpec == null) {
                            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                                String.format("Unknown partition '%s' in table '%s'", partName,
                                    partInfo.getTableName()));
                        }
                        boolean isPhySpec = !pSpec.isLogical();
                        if (isPhySpec) {
                            selectedPartPostSet.add(pSpec.getPhyPartPosition().intValue());
                        } else {
                            if (pSpec.isSpecTemplate()) {
                                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                                    String.format("Not allowed to select partition by using subpartition template '%s'",
                                        partName,
                                        partInfo.getTableName()));
                            }

                            List<PartitionSpec> subPartList = pSpec.getSubPartitions();
                            for (int j = 0; j < subPartList.size(); j++) {
                                PartitionSpec subPart = subPartList.get(j);
                                selectedPartPostSet.add(subPart.getPhyPartPosition().intValue());
                            }
                        }

                    }
                    BitSet partSetSelected =
                        PartitionPrunerUtils.buildPhyPartsBitSetByPhyPartPostSet(partInfo, selectedPartPostSet);
                    resultList.get(i).getPartBitSet().and(partSetSelected);
                } else if (partInfo.getTableType() == PartitionTableType.BROADCAST_TABLE) {
                    continue;
                } else {
                    throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                        "PARTITION () clause on non partitioned table");
                }
            }

        }
    }

    protected Map<String, List<List<String>>> buildTargetTablesForShardDbTb(ExecutionContext executionContext) {
        boolean forceAllowFullTableScan =
            executionContext.getParamManager().getBoolean(ConnectionParams.ALLOW_FULL_TABLE_SCAN);
        List<List<TargetDB>> targetDBs;
        if (null == comparativeHintCache) {
            targetDBs = DataNodeChooser.shard(this, forceAllowFullTableScan, executionContext);
        } else {
            final BitSet evaluatedParamIndex = new BitSet();
            this.comparativeHintCache.values().forEach(e -> e.values().forEach(
                comparative ->
                    RexUtils.updateParam(
                        comparative,
                        executionContext,
                        (r) -> !evaluatedParamIndex.get(r.getIndex()),
                        (r) -> evaluatedParamIndex.set(r.getIndex()))));

            targetDBs =
                DataNodeChooser.shard(this, this.comparativeHintCache, forceAllowFullTableScan, executionContext);
        }

        //final Set<String> groupIntersection = getGroupIntersection(targetDBs, schemaName);
        //targetDBs = filterGroup(targetDBs, groupIntersection, schemaName);
        Map<String, List<List<String>>> result = PlannerUtils.convertTargetDB(targetDBs, schemaName, crossSingleTable,
            executionContext);

        if (GeneralUtil.isEmpty(result)) {
            if (null == comparativeHintCache) {
                logger.warn("Empty target tables got, use full table scan for instead. logical tables: "
                    + TStringUtil.join(this.tableNames.toArray()));

                // build full table scan
                targetDBs = DataNodeChooser.shard(this,
                    this.tableNames.stream().distinct().collect(Collectors.toMap(tn -> tn, tn -> new HashMap<>())),
                    forceAllowFullTableScan,
                    executionContext);

                //final Set<String> gi = getGroupIntersection(targetDBs, schemaName);
                //targetDBs = filterGroup(targetDBs, gi, schemaName);
                result = PlannerUtils.convertTargetDB(targetDBs, schemaName, crossSingleTable);
            } else {
                throw new TddlRuntimeException(ERR_TABLE_EMPTY_WITH_HINT);
            }
        }

        return result;
    }

    protected Map<String, Object> getExtraCmds() {
        return null;
    }

    /**
     * update Comparative cache, for HINT ONLY!
     */
    public void setComparativeHintCache(Map<String, Map<String, Comparative>> comparativeHintCache) {
        this.comparativeHintCache = comparativeHintCache;
    }

    /**
     * update PartitionPruneStep cache, for HINT ONLY!
     */
    public void setPruneStepHintCache(Map<String, PartitionPruneStep> pruneStepHintCache) {
        this.pruneStepHintCache = pruneStepHintCache;
    }

    /**
     * update TargetTables cache, for HINT ONLY!
     */
    public void setTargetTables(Map<String, List<List<String>>> targetTables) {
        this.targetTablesHintCache = targetTables;
    }

    public SqlNode getSqlTemplate(ExecutionContext executionContext) {
        return getSqlTemplate(null, executionContext);
    }

    public SqlNode getSqlTemplate(ReplaceCallWithLiteralVisitor visitor, ExecutionContext ec) {
        boolean usingCache = true;
        if (ec != null) {
            usingCache = ec.enablePhySqlCache();
        }
        if (sqlTemplateHintCache == null) {
            if (visitor == null && scalarList.isEmpty() && correlateVariableScalar.isEmpty()) {
                if (!usingCache || sqlTemplateCache == null) {
                    sqlTemplateCache = buildSqlTemplate(null, ec);
                }
                return sqlTemplateCache;
            } else {
                return buildSqlTemplate(visitor, ec);
            }
        } else {
            return sqlTemplateHintCache;
        }
    }

    public SqlNode getSqlTemplate(ReplaceCallWithLiteralVisitor visitor) {
        return getSqlTemplate(visitor, null);
    }

    /**
     * update SqlTemplate cache, for HINT ONLY!
     */
    public SqlNode setSqlTemplate(SqlNode nativeSqlNode) {
        ReplaceTableNameWithQuestionMarkVisitor visitor = new ReplaceTableNameWithQuestionMarkVisitor(schemaName,
            PlannerContext.getPlannerContext(this).getExecutionContext());
        SqlNode sqlTemplate = nativeSqlNode.accept(visitor);
        if (!visitor.getTableNames().isEmpty()) {
            tableNames.clear();
            tableNames.addAll(visitor.getTableNames());
        }
        this.sqlTemplateHintCache = sqlTemplate;
        return sqlTemplateHintCache;
    }

    public String getSqlTemplateStr(ExecutionContext ec) {
        if (isCacheForbidden()) {
            return RelUtils.toNativeSql(getSqlTemplate(ec), dbType);
        }
        if (sqlTemplateStringCache == null) {
            sqlTemplateStringCache = RelUtils.toNativeSql(getSqlTemplate(ec), dbType);
        }
        return sqlTemplateStringCache;
    }

    public BytesSql getBytesSql(SqlNode sqlNode) {
        if (isCacheForbidden()) {
            return RelUtils.toNativeBytesSql(sqlNode, dbType);
        }
        if (bytesSql == null) {
            bytesSql = RelUtils.toNativeBytesSql(sqlNode, dbType);
        }
        return bytesSql;
    }

    protected SqlNode buildSqlTemplate(ReplaceCallWithLiteralVisitor replaceCallWithLiteralVisitor,
                                       ExecutionContext ec) {
        SqlNode sqlTemplate = getNativeSqlNode(replaceCallWithLiteralVisitor);

        // process optimizer hint
        if (sqlTemplate instanceof SqlSelect) {
            float samplePercentage =
                PlannerContext.getPlannerContext(this).getParamManager().getFloat(ConnectionParams.SAMPLE_PERCENTAGE);
            if (samplePercentage >= 0 && samplePercentage <= 100) {
                ((SqlSelect) sqlTemplate).getOptimizerHint().addHint("+sample_percentage(" + samplePercentage + ")");
            }

            // Pass through user hint.
            if (getHintContext() != null && !getHintContext().getHints().isEmpty()) {
                for (String hint : getHintContext().getHints()) {
                    ((SqlSelect) sqlTemplate).getOptimizerHint().addHint(hint);
                }
            }
        }

        if (ec == null) {
            /**
             *  If phy sql contains any scalar subquery, then ec should NOT be null
             */
            ec = PlannerContext.getPlannerContext(this).getExecutionContext();
        }
        ReplaceTableNameWithQuestionMarkVisitor visitor = new ReplaceTableNameWithQuestionMarkVisitor(schemaName, ec);
        return sqlTemplate.accept(visitor);
    }

    public SqlNode getNativeSqlNode() {
        return getNativeSqlNode(null);
    }

    public SqlNode getNativeSqlNode(ReplaceCallWithLiteralVisitor visitor) {
        RelToSqlConverter converter = getConverter();
        SqlNode sqlNode = pushDownOpt.getNativeSql(converter, visitor);
        if (sqlNode instanceof SqlSelect) {
            ((SqlSelect) sqlNode).setLockMode(lockMode);
        }
        return sqlNode;
    }

    protected RelToSqlConverter getConverter() {
        return TddlRelToSqlConverter.createInstance(dbType);
    }

    public XPlanTemplate getXPlanDirect() {
        return XPlan;
    }

    public XPlanTemplate getXPlan() {
        // Always generate the XPlan in case of switching connection pool.
        if (lockMode != LockMode.UNDEF) {
            return null; // TODO: lock not supported now.
        }
        final RelNode pushedRel = getPushedRelNode();
        if (XPlanRel != pushedRel) { // Compare the value to check whether the plan changed.
            final RelToXPlanConverter converter = new RelToXPlanConverter();
            try {
                XPlan = converter.convert(RelXPlanOptimizer.optimize(pushedRel));
            } catch (Exception e) {
                Throwable throwable = e;
                while (throwable.getCause() != null && throwable.getCause() instanceof InvocationTargetException) {
                    throwable = ((InvocationTargetException) throwable.getCause()).getTargetException();
                }
                logger.info("XPlan converter: " + throwable.getMessage());
                XPlan = null;
            }
            XPlanRel = pushedRel;
        }
        return XPlan;
    }

    public boolean isSupportGalaxyPrepare() {
        final RelNode pushedRel = getPushedRelNode();
        if (galaxyPrepareRel != pushedRel) { // Compare the value to check whether the plan changed.
            final SpecialFunctionRelFinder finder = new SpecialFunctionRelFinder();
            finder.go(pushedRel);
            supportGalaxyPrepare = finder.supportGalaxyPrepare();
            galaxyPrepareRel = pushedRel;
        }
        return supportGalaxyPrepare;
    }

    public ByteString getGalaxyPrepareDigest(ExecutionContext executionContext, BytesSql sqlTemplate) {
        if (!sqlTemplate.equals(galaxyPrepareSqlTemplateCache)) {
            // Only generate once.
            galaxyPrepareSqlTemplateCache = sqlTemplate;
            // Generate digest with schema name, table name(with table version), sql template.
            galaxyPrepareDigest =
                Planner.calcGalaxyPrepareDigest(schemaName, sqlTemplate, tableNames, executionContext);
        }
        return galaxyPrepareDigest;
    }

    public boolean isXPlanValid() {
        return getXPlan() != null;
    }

    public void push(RelNode relNode) {
        if (relNode instanceof LogicalInsert) {
            this.hints = ((LogicalInsert) relNode).getHints();
        } else if (relNode instanceof Project) {
            RexUtil.DynamicFinder dynamicFinder = new RexUtil.DynamicFinder();
            for (RexNode rexNode : ((Project) relNode).getProjects()) {
                rexNode.accept(dynamicFinder);
            }
            Map<RexNode, RexNode> replacements =
                dynamicFinder.getScalar().stream().filter(e -> isApplyPushable(e))
                    .collect(Collectors.toMap(e -> e, e -> applyToSubquery(e)));

            // correlate subquery must be single table
            replacements.putAll(dynamicFinder.getCorrelateScalar().stream()
                .collect(Collectors.toMap(e -> e, e -> applyToSubquery(e))));

            relNode = rebuildProject(replacements, (Project) relNode);

            // avoid redundant calculations for apply subquery
            dynamicFinder.getScalar().removeAll(replacements.keySet());
            dynamicFinder.getCorrelateScalar().removeAll(replacements.keySet());

            scalarList.addAll(dynamicFinder.getScalar());
            correlateVariableScalar.addAll(dynamicFinder.getCorrelateVariableScalar());
        } else if (relNode instanceof Filter) {
            RexUtil.DynamicFinder dynamicFinder = new RexUtil.DynamicFinder();
            ((Filter) relNode).getCondition().accept(dynamicFinder);

            Map<RexNode, RexNode> replacements =
                dynamicFinder.getScalar().stream().filter(e -> isApplyPushable(e))
                    .collect(Collectors.toMap(e -> e, e -> applyToSubquery(e)));

            // correlate subquery must be single table
            replacements.putAll(dynamicFinder.getCorrelateScalar().stream()
                .collect(Collectors.toMap(e -> e, e -> applyToSubquery(e))));

            relNode = rebuildFilter(replacements, (Filter) relNode);

            // avoid redundant calculations for apply subquery
            dynamicFinder.getScalar().removeAll(replacements.keySet());
            dynamicFinder.getCorrelateScalar().removeAll(replacements.keySet());

            scalarList.addAll(dynamicFinder.getScalar());
            correlateVariableScalar.addAll(dynamicFinder.getCorrelateVariableScalar());
        }
        pushDownOpt.push(relNode);
        tableNames = collectTableNames();
    }

    /**
     * rebuild filter by replacing apply to subquery rexnode in condition
     */
    private RelNode rebuildFilter(Map<RexNode, RexNode> replacements, Filter relNode) {
        RexNode condition = relNode.getCondition();
        for (Entry<RexNode, RexNode> replace : replacements.entrySet()) {
            RexUtil.ReplaceDynamicParamsShuttle
                replaceDynamicParamsShuttle = new RexUtil.ReplaceDynamicParamsShuttle(
                (RexDynamicParam) replace.getKey(), replace.getValue());
            condition = condition.accept(replaceDynamicParamsShuttle);
        }

        return new LogicalFilter(relNode.getCluster(), relNode.getTraitSet(), relNode.getInput(), condition,
            ImmutableSet.copyOf(relNode.getVariablesSet()));
    }

    /**
     * Transform apply to subquery
     *
     * @param e : subquery in apply mode
     * @return rexsubquery
     */
    private RexNode applyToSubquery(RexDynamicParam e) {
        RelNode orgin = pushDownOpt.optimize(extractRelFromLogicalView(e.getRel()));
        switch (e.getSemiType()) {
        case LEFT:
            return RexSubQuery.scalar(orgin);
        case SEMI:
            if (e.getLeftCondition() != null && e.getLeftCondition().size() > 0) {
                return RexSubQuery.some(orgin, ImmutableList.copyOf(e.getLeftCondition()),
                    SqlStdOperatorTable.some(e.getSubqueryKind()));
            } else {
                return RexSubQuery.exists(orgin);
            }
        case ANTI:
            if (e.getLeftCondition() != null && e.getLeftCondition().size() > 0) {
                return RexSubQuery.all(orgin, ImmutableList.copyOf(e.getLeftCondition()),
                    SqlStdOperatorTable.all(e.getSubqueryKind()));
            } else {
                return RexSubQuery.not_exists(orgin);
            }
        default:
            throw new NotSupportException(e.getSubqueryOp().getKind().toString());
        }
    }

    private RelNode extractRelFromLogicalView(RelNode rel) {
        if (rel instanceof LogicalView) {
            return ((LogicalView) rel).getPushedRelNode();
        } else {
            List<RelNode> newInputs =
                rel.getInputs().stream().map(e -> extractRelFromLogicalView(e)).collect(Collectors.toList());
            return rel.copy(rel.getTraitSet(), newInputs);
        }
    }

    private boolean isApplyPushable(RexDynamicParam e) {
        if (OptimizerUtils.hasApply(e.getRel())) {
            return false;
        }
        Set<RelOptTable> tables = RelOptUtil.findTables(e.getRel());
        tables.addAll(RelOptUtil.findTables(getPushedRelNode()));
        return TableTopologyUtil.isAllSingleTableInSamePhysicalDB(tables);
    }

    private RelNode rebuildProject(Map<RexNode, RexNode> replacements, Project project) {
        List<RexNode> replacePros = Lists.newLinkedList();
        for (RexNode pro : project.getProjects()) {
            for (Entry<RexNode, RexNode> replace : replacements.entrySet()) {
                RexUtil.ReplaceDynamicParamsShuttle
                    replaceDynamicParamsShuttle = new RexUtil.ReplaceDynamicParamsShuttle(
                    (RexDynamicParam) replace.getKey(), replace.getValue());
                pro = pro.accept(replaceDynamicParamsShuttle);
            }
            replacePros.add(pro);
        }

        return new LogicalProject(getCluster(), getTraitSet(), project.getInput(), replacePros,
            project.getRowType(), project.getOriginalRowType(), project.getVariablesSet());
    }

    public void pushJoin(Join join, LogicalView rightView, List<RexNode> leftFilters, List<RexNode> rightFilters) {
        pushDownOpt.pushJoin(join, rightView, leftFilters, rightFilters, getCluster());
        tableNames = collectTableNames();
        mergeHints(join, rightView);
        rebuildPartRoutingPlanInfo();
    }

    public void pushSemiJoinDirect(LogicalSemiJoin join, LogicalView rightView, List<RexNode> leftFilters,
                                   List<RexNode> rightFilters) {
        pushDownOpt.pushSemiJoinDirect(join, rightView, leftFilters, rightFilters, getCluster());
        tableNames = collectTableNames();
        mergeHints(join, rightView);
        rebuildPartRoutingPlanInfo();
    }

    public void pushSemiJoin(LogicalSemiJoin join, LogicalView rightView, List<RexNode> leftFilters,
                             List<RexNode> rightFilters, RelNode relNode) {
        pushDownOpt.pushSemiJoin(join, rightView, leftFilters, rightFilters, getCluster());
        push(relNode);
        tableNames = collectTableNames();
        mergeHints(join, rightView);
        rebuildPartRoutingPlanInfo();
    }

    /**
     * merge hints from left and right table
     */
    private void mergeHints(Join join, LogicalView rightView) {
        if (this.emptyHints()) {
            if (!join.emptyHints()) {
                this.setHints(join.getHints());
            } else if (!rightView.emptyHints()) {
                this.setHints(rightView.getHints());
            }
        }
    }

    public boolean aggIsPushed() {
        return pushDownOpt.aggIsPushed();
    }

    public RelWriter explainLogicalView(RelWriter pw) {
        pw.item(RelDrdsWriter.REL_NAME, explainNodeName());
        if (join != null) {
            Index index = getLookupJoin().getLookupIndex();
            if (index != null) {
                pw.item("joinIndex", index.getIndexMeta().getPhysicalIndexName());
            }
        }
        List<RelNode> relList = new ArrayList<>();
        relList.add(getMysqlNode());
        pw.item(RelDrdsWriter.LV_INPUTS, relList);
        return pw;
    }

    @Override
    public RelWriter explainTermsForDisplay(RelWriter pw) {

        if (PlannerContext.getPlannerContext(this).getParamManager().getBoolean(ConnectionParams.EXPLAIN_LOGICALVIEW)) {
            return explainLogicalView(pw);
        }

        pw.item(RelDrdsWriter.REL_NAME, explainNodeName());
        SqlNode nativeSql = getNativeSqlNode();
        int shardCount = 0;
        /**
         * the params of explain sql ars passed by RelWriter.
         */
        final Map<Integer, ParameterContext> params;
        ExecutionContext executionContext = null;
        if (pw instanceof RelDrdsWriter) {
            params = ((RelDrdsWriter) pw).getParams();
            executionContext = (ExecutionContext) ((RelDrdsWriter) pw).getExecutionContext();
        } else if (pw instanceof RelDrdsJsonWriter) {
            params = ((RelDrdsJsonWriter) pw).getParams();
            executionContext = (ExecutionContext) ((RelDrdsWriter) pw).getExecutionContext();
        } else {
            throw new AssertionError();
        }
        // Always allow full table scan for explain
        if (executionContext == null) {
            executionContext = new ExecutionContext();
            if (schemaName != null) {
                executionContext.setSchemaName(schemaName);
            }
            if (params != null) {
                executionContext.setParams(new Parameters(params));
            }
        }
        executionContext.getExtraCmds().put(ConnectionProperties.ALLOW_FULL_TABLE_SCAN, true);

        String phyTableString = null;

        if (!newPartDbTbl) {
            // don't use cache when using 'explain optimizer'
            List<RelNode> phyTableScans = executionContext.getCalcitePlanOptimizerTrace().isPresent() ?
                getInputWithoutCache(executionContext) : getInput(executionContext);
            if (phyTableScans.isEmpty()) {
                shardCount = 1;
            } else {
                for (RelNode phyTable : phyTableScans) {
                    if (phyTable instanceof PhyTableOperation && null != ((PhyTableOperation) phyTable)
                        .getTableNames()) {
                        shardCount += ((PhyTableOperation) phyTable).getTableNames().size();
                    } else {
                        shardCount += 1;
                    }
                }
            }
            phyTableString = ExplainUtils.getPhyTableString(getTableNames(), phyTableScans);
        } else {
            List<PartPrunedResult> resultList = new ArrayList<>();
            if (null == this.targetTablesHintCache) {
                resultList = PartitionPruner.prunePartitions(this, executionContext);
                filterPrunedResultBySelectedPartitions(resultList);
            } else {
                buildPrunedResultByTargetTablesHintCache(resultList);
            }

            if (resultList.size() == 0) {
                shardCount = 0;
                phyTableString = "[]";
            } else {
                phyTableString = "";
                for (int i = 0; i < resultList.size(); i++) {
                    if (i > 0) {
                        phyTableString += ",";
                    }
                    PartPrunedResult rs = resultList.get(i);
                    String logTbName = rs.getLogicalTableName();
                    phyTableString += logTbName;
                    if (rs.getPartInfo().isBroadcastTable()) {
                        continue;
                    }
                    phyTableString += "[";
                    List<PhysicalPartitionInfo> prunedParts = rs.getPrunedParttions();
                    shardCount = prunedParts.size();
                    if (shardCount > 10) {
                        phyTableString += prunedParts.get(0).getPartName() + ",";
                        phyTableString += prunedParts.get(1).getPartName() + ",";
                        phyTableString += prunedParts.get(2).getPartName() + ",";
                        phyTableString += "...";
                        phyTableString += prunedParts.get(shardCount - 1).getPartName();
                    } else {
                        for (int j = 0; j < prunedParts.size(); j++) {
                            if (j > 0) {
                                phyTableString += ",";
                            }
                            String partName = prunedParts.get(j).getPartName();
                            phyTableString += partName;

                        }
                    }
                    phyTableString += "]";
                }
            }
        }

        pw.itemIf("tables", phyTableString, phyTableString != null);
        if (shardCount > 1 || shardCount == 0) {
            pw.item("shardCount", shardCount);
        }

        pw.itemIf("partition", traitSet.getPartitionWise(), !traitSet.getPartitionWise().isTop());

        if (isMGetEnabled && join != null) {
            List<LookupEquiJoinKey> joinKeys =
                EquiJoinUtils.buildLookupEquiJoinKeys(join, join.getOuter(), join.getInner(),
                    (RexCall) join.getCondition(), join.getJoinType());
            LookupPredicate predicate = new LookupPredicateBuilder(join, columnOrigins).build(joinKeys);
            SqlNode lookupPredicate = predicate.explain();

            SqlNode filter = ((SqlSelect) nativeSql).getWhere();
            if (filter != null) {
                SqlOperator operator = SqlStdOperatorTable.AND;
                filter = new SqlBasicCall(operator, new SqlNode[] {filter, lookupPredicate}, SqlParserPos.ZERO);
            } else {
                filter = lookupPredicate;
            }
            ((SqlSelect) nativeSql).setWhere(filter);
        }
        ReplaceTableNameWithTestTableVisitor replaceTableNameWithTestTableVisitor =
            new ReplaceTableNameWithTestTableVisitor(schemaName, false,
                PlannerContext.getPlannerContext(this).getExecutionContext());
        nativeSql = nativeSql.accept(replaceTableNameWithTestTableVisitor);
        String sql = TStringUtil.replace(RelUtils.toNativeSql(nativeSql, this.getDbType()), "\n", " ");

        pw.item("sql", sql);

        // TODO: RT filter disabled now.
//        if (isMGetEnabled && join != null && join instanceof HashJoin) {
//            pw.item("runtimeFilter", true);
//        }

        final XPlanTemplate XPlan = getXPlan();
        if (XPlan != null && executionContext.getParamManager().getBoolean(ConnectionParams.EXPLAIN_X_PLAN)) {
            final JsonFormat format = new JsonFormat();
            final PolarxExecPlan.ExecPlan plan = XPlan.explain(executionContext);
            if (null == plan) {
                pw.item("XPlan", "Denied by param.");
            } else {
                pw.item("XPlan", format.printToString(plan));
            }
        }
        String pruningInfo = pruningInfo(executionContext);
        boolean couldPruning = pushDownOpt.couldDynamicPruning();
        pw.itemIf("pruningInfo", pruningInfo, couldPruning && StringUtils.isNotEmpty(pruningInfo));

        // FIXME generate correct param for LogicalView
        // StringBuilder builder = new StringBuilder();
        // if (MapUtils.isNotEmpty(params)) {
        // String operator = "";
        // for (Object c : params.values()) {
        // Object v = ((ParameterContext) c).getValue();
        // builder.append(operator);
        // if (v instanceof TableName) {
        // builder.append(((TableName) v).getTableName());
        // } else {
        // builder.append(v.toString());
        // }
        // operator = ",";
        // }
        // pw.item("params", builder.toString());
        // }

        return pw;
    }

    /**
     * get pruning info, only for explain
     */
    private String pruningInfo(ExecutionContext executionContext) {
        Set<Integer> indexes = pushDownOpt.getShardRelatedInTypeParamIndexes();
        if (indexes == null || indexes.size() == 0) {
            return "";
        }
        Map<com.alibaba.polardbx.common.utils.Pair<String, List<String>>, Parameters> map =
            OptimizerUtils.pruningInValue(this, executionContext);
        if (map == null || map.size() == 0) {
            return "";
        }
        int fullInFullSize = 0;
        int pruningSize = 0;
        Parameters allParameters = executionContext.getParams();

        for (Integer index : indexes) {
            if (allParameters.getCurrentParameter().containsKey(index) &&
                allParameters.getCurrentParameter().get(index).getValue() instanceof RawString) {
                RawString rawString = (RawString) allParameters.getCurrentParameter().get(index).getValue();
                fullInFullSize += rawString.size();
            }
        }

        StringBuilder detail = new StringBuilder(", pruning detail:");
        for (Map.Entry<com.alibaba.polardbx.common.utils.Pair<String, List<String>>, Parameters> entry : map.entrySet()) {

            detail.append(entry.getKey().toString()).append("->(");
            Parameters parameters = entry.getValue();
            int pruningInSize = 0;
            for (Integer index : indexes) {
                if (parameters.getCurrentParameter().containsKey(index) &&
                    parameters.getCurrentParameter().get(index).getValue() instanceof RawString) {
                    RawString rawString = (RawString) parameters.getCurrentParameter().get(index).getValue();
                    pruningInSize += rawString.size();
                    detail.append(rawString.display()).append(",");
                }
            }
            if (detail.charAt(detail.length() - 1) == ',') {
                detail.deleteCharAt(detail.length() - 1);
            }
            detail.append(");");
            pruningSize += fullInFullSize - pruningInSize;
        }
        if (pruningSize == 0) {
            return "";
        }

        String info = "all size:" + fullInFullSize + "*" + map.values().size() + "(part), pruning size:" + pruningSize;
        boolean outputDetail = executionContext.getParamManager().getBoolean(ConnectionParams.EXPLAIN_PRUNING_DETAIL);
        if (outputDetail) {
            if (detail.charAt(detail.length() - 1) == ';') {
                detail.deleteCharAt(detail.length() - 1);
            }
            info = info + ", pruning time:" + executionContext.getPruningTime() + "ms" + detail;
        }
        return info;
    }

    public void setIsMGetEnabled(boolean isMGetEnabled) {
        this.isMGetEnabled = isMGetEnabled;
    }

    public boolean isMGetEnabled() {
        return isMGetEnabled;
    }

    public boolean getFinishShard() {
        return finishShard;
    }

    public void setFinishShard(boolean matched) {
        this.finishShard = matched;
    }

    public List<String> getTableNames() {
        return tableNames;
    }

    public RelNode getPushedRelNode() {
        return pushDownOpt.getPushedRelNode();
    }

    public RelNode getSemiJoinRemovedRelNode() {
        // in case of wrong cache, ignore it directly when disabling subquery_unwrap
        if (!PlannerContext.getPlannerContext(pushDownOpt.getPushedRelNode()).getParamManager()
            .getBoolean(ConnectionParams.ENABLE_LV_SUBQUERY_UNWRAP)) {
            return pushDownOpt.getPushedRelNode();
        }

        return pushDownOpt.removeSemiJoin();
    }

    public RelNode getMysqlNode() {
        if (mysqlNodeCache == null
            || optimizedPushedRelNodeForMetaQueryCache != getOptimizedPushedRelNodeForMetaQuery()) {
            synchronized (this) {
                if (mysqlNodeCache == null
                    || optimizedPushedRelNodeForMetaQueryCache != getOptimizedPushedRelNodeForMetaQuery()) {
                    mysqlNodeCache = CBOUtil.optimizeByMysqlImpl(this.getOptimizedPushedRelNodeForMetaQuery());
                }
            }
        }
        return mysqlNodeCache;
    }

    public RelNode getOptimizedPushedRelNodeForMetaQuery() {
        if (lastPushedRelNodeCache == null || lastPushedRelNodeCache != getPushedRelNode()
            || optimizedPushedRelNodeForMetaQueryCache == null) {
            synchronized (this) {
                if (lastPushedRelNodeCache == null || lastPushedRelNodeCache != getPushedRelNode()
                    || optimizedPushedRelNodeForMetaQueryCache == null) {
                    lastPushedRelNodeCache = pushDownOpt.getPushedRelNode();
                    optimizedPushedRelNodeForMetaQueryCache = pushDownOpt.optimize(lastPushedRelNodeCache);

                    CountVisitor countVisitor = new CountVisitor();
                    countVisitor.visit(getPushedRelNode());

                    HepProgramBuilder builder = new HepProgramBuilder();
                    builder.addGroupBegin();
                    builder.addRuleInstance(SubQueryToSemiJoinRule.FILTER);
                    builder.addRuleInstance(SubQueryToSemiJoinRule.PROJECT);
                    builder.addGroupEnd();
                    if (countVisitor.getJoinCount() < PlannerContext.getPlannerContext(this).getParamManager()
                        .getInt(ConnectionParams.CBO_LEFT_DEEP_TREE_JOIN_LIMIT)) {
                        builder.addGroupBegin();
                        builder.addRuleInstance(FilterJoinRule.JOIN);
                        builder.addRuleInstance(TddlFilterJoinRule.TDDL_FILTER_ON_JOIN);
                        builder.addRuleInstance(FilterProjectTransposeRule.INSTANCE);
                        builder.addRuleInstance(FilterMergeRule.INSTANCE);
                        builder.addGroupEnd();
                    }
                    builder.addGroupBegin();
                    builder.addRuleInstance(JoinConditionSimplifyRule.INSTANCE);
                    builder.addGroupEnd();
                    HepPlanner planner = new HepPlanner(builder.build());
                    planner.stopOptimizerTrace();
                    planner.setRoot(lastPushedRelNodeCache);
                    optimizedPushedRelNodeForMetaQueryCache = planner.findBestExp();
                }
            }
        }
        return optimizedPushedRelNodeForMetaQueryCache;
    }

    public boolean pushedRelNodeIsSort() {
        RelNode pushedRelNode = this.getOptimizedPushedRelNodeForMetaQuery();
        if (pushedRelNode != null && pushedRelNode instanceof Sort) {
            Sort sort = (Sort) pushedRelNode;
            if (sort.getCollation().getFieldCollations().size() > 0) {
                return true;
            }
        }
        return false;
    }

    public String getLogicalTableName() {
        return getTableNames().get(0);
    }

    public RelShardInfo getRelShardInfo(ExecutionContext ec) {
        return pushDownOpt.getRelShardInfo(0, ec);
    }

    public RelShardInfo getRelShardInfo(int tableIndex, ExecutionContext ec) {
        final String logTbName = getTableNames().get(tableIndex);
        // Skip pushdown hint if partition hint exists
        if (useSelectPartitions() || null == pruneStepHintCache || !pruneStepHintCache.containsKey(logTbName)) {
            return pushDownOpt.getRelShardInfo(tableIndex, ec);
        } else {
            final PartitionPruneStep partitionPruneStep = pruneStepHintCache.get(logTbName);

            RexUtils.updateParam(
                partitionPruneStep,
                ec,
                (r) -> !RexUtils.paramExists(r.getIndex() + 1, ec),
                (r) -> {
                });

            final RelShardInfo relShardInfo = new RelShardInfo();
            relShardInfo.setTableName(logTbName);
            relShardInfo.setSchemaName(getSchemaName());
            relShardInfo.setUsePartTable(true);
            relShardInfo.setPartPruneStepInfo(partitionPruneStep);

            return relShardInfo;
        }
    }

    public Map<String, Comparative> getComparative() {
        return pushDownOpt.getComparative(0);
    }

    public Map<String, Comparative> getComparative(int tableIndex) {
        return pushDownOpt.getComparative(tableIndex);
    }

    public Map<String, Comparative> getFullComparative(int tableIndex) {
        return pushDownOpt.getFullComparative(tableIndex);
    }

    public Map<String, Comparative> getFullComparativeCopy(int tableIndex) {
        Map<String, Comparative> originalOne = getFullComparative(tableIndex);
        if (originalOne == null) {
            return null;
        } else {
            Map<String, Comparative> copyOne = new HashMap<>();
            for (Entry<String, Comparative> entry : originalOne.entrySet()) {
                copyOne.put(entry.getKey(), entry.getValue().clone());
            }
            return copyOne;
        }
    }

    public void setTableName(List<String> tableNames) {
        this.tableNames = tableNames;
    }

    public boolean isJoin() {
        return tableNames.size() > 1;
    }

    public void setJoin(Join join) {
        assert join instanceof LookupJoin;
        this.join = join;
        rebuildOriginColumnNames();
    }

    public Join getJoin() {
        return join;
    }

    public LookupJoin getLookupJoin() {
        return (LookupJoin) join;
    }

    public DbType getDbType() {
        return dbType;
    }

    public List<Pair<Integer, String>> getPlainRefIndex() {
        return pushDownOpt.getPlainRefIndex();
    }

    public int getRefByColumnName(String tableName, String columnName, boolean last, boolean ignoreDerive) {
        return pushDownOpt.getRefByColumnName(tableName, columnName, last, ignoreDerive);
    }

    /**
     * <pre>
     * 历经投影的下推，会导致当前LogicalView输出的列与原始的列不同，在此构建真实输出的列
     *
     * 有很多情况,会导致 RefIndex 为 -1,
     * 这种情况下说明该列是常量或引用多个列,其不可能唯一映射至下层输入节点的一列
     * </pre>
     */
    public RelDataType buildCurRowType() {
        return pushDownOpt.buildCurRowType();
    }

    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
        return pushDownOpt.estimateRowCount(mq);
    }

    @Override
    public RelDataType deriveRowType() {
        if (null != this.pushDownOpt) {
            RelNode pushedNode = getPushedRelNode();
            if (pushedNode == null) {
                return super.deriveRowType();
            }
            return pushedNode.getRowType();
        }

        return super.deriveRowType();
    }

    public int calShardUpperBound() {
        TddlRuleManager ruleManager =
            PlannerContext.getPlannerContext(this).getExecutionContext().getSchemaManager(schemaName)
                .getTddlRuleManager();
        PartitionInfoManager partitionInfoManager = ruleManager.getPartitionInfoManager();

        String logTb = getShardingTable();
        List<String> shardColumns = ruleManager.getSharedColumns(logTb);
        if (shardColumns == null || shardColumns.isEmpty()) {
            return 1;
        }

        boolean calActualShardCount =
            PlannerContext.getPlannerContext(this).getParamManager()
                .getBoolean(ConnectionParams.CALCULATE_ACTUAL_SHARD_COUNT_FOR_COST);
        ExecutionContext executionContext = PlannerContext.getPlannerContext(this).getExecutionContext();
        if (calActualShardCount) {
            try {
                if (!partitionInfoManager.isNewPartDbTable(logTb)) {
                    int shardingTableIndex = 0;
                    for (int i = 0; i < getTableNames().size(); i++) {
                        String tableName = getTableNames().get(i);
                        if (tableName.equalsIgnoreCase(logTb)) {
                            shardingTableIndex = i;
                            break;
                        }
                    }
                    RelShardInfo relShardInfo = getRelShardInfo(shardingTableIndex, executionContext);
                    Map<String, Comparative> comps = relShardInfo.getAllComps();
                    Map<String, Object> calcParams = new HashMap<>();
                    calcParams.put(CalcParamsAttribute.SHARD_FOR_EXTRA_DB, false);
                    Map<String, Map<String, Comparative>> m = Maps.newHashMap();
                    m.put(relShardInfo.getTableName(), relShardInfo.getAllFullComps());
                    calcParams.put(CalcParamsAttribute.COM_DB_TB, m);
                    calcParams.put(CalcParamsAttribute.CONN_TIME_ZONE, executionContext.getTimeZone());
                    calcParams.put(CalcParamsAttribute.EXECUTION_CONTEXT, executionContext);
                    Map<Integer, ParameterContext> params =
                        executionContext.getParams() == null ? null :
                            executionContext.getParams().getCurrentParameter();
                    List<TargetDB> tdbs =
                        executionContext.getSchemaManager(schemaName).getTddlRuleManager()
                            .shard(getShardingTable(), false, true, comps, params, calcParams, executionContext);

                    return tdbs.stream().map(targetDB -> targetDB.getTableNames() == null ? 0 :
                        targetDB.getTableNames().size()).reduce(0, (a, b) -> a + b).intValue();

                } else {
                    return PartitionPruner.doPruningByStepInfo(getRelShardInfo(executionContext).getPartPruneStepInfo(),
                        executionContext).getPartBitSet().cardinality();
                }
            } catch (Throwable t) {
                // params might be clear, pass
            }
        }
        int totalShardCount = getTotalShardCount();

        return PlannerUtils.guessShardCount(shardColumns, getRelShardInfo(executionContext), totalShardCount);
    }

    public int getTotalShardCount() {
        TddlRuleManager ruleManager =
            PlannerContext.getPlannerContext(this).getExecutionContext().getSchemaManager(schemaName)
                .getTddlRuleManager();
        PartitionInfoManager partitionInfoManager = ruleManager.getPartitionInfoManager();
        String logTb = getShardingTable();
        int totalShardCount = 0;
        if (!partitionInfoManager.isNewPartDbTable(logTb)) {
            TableRule tr = ruleManager.getTableRule(logTb);
            Map<String, Set<String>> actualTopology = tr.getActualTopology();
            for (Set<String> s : actualTopology.values()) {
                totalShardCount += s.size();
            }
            if (tr.getExtPartitions() != null && tr.getExtPartitions().size() > 0) {
                return totalShardCount;
            }
        } else {
            PartitionInfo partInfo = partitionInfoManager.getPartitionInfo(logTb);
            totalShardCount = partInfo.getAllPhysicalPartitionCount();
        }
        return totalShardCount;
    }

    public boolean isSingleGroupSingleTable() {

        if (PlannerUtils.allTableSingle(tableNames, schemaName,
            PlannerContext.getPlannerContext(this).getExecutionContext().getSchemaManager(schemaName)
                .getTddlRuleManager())) {
            return true;
        } else {
            return false;
        }
    }

    /**
     *
     */
    public String calculateAllSingleTableDNId() {
        ExecutionContext ec = PlannerContext.getPlannerContext(this).getExecutionContext();
        TddlRuleManager tddlRuleManager = ec.getSchemaManager(schemaName).getTddlRuleManager();
        if (DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
            for (String table : tableNames) {
                // Check if all tables are single table or broadcast table
                PartitionInfo partInfo = tddlRuleManager.getPartitionInfoManager().getPartitionInfo(table);
                if (partInfo != null) {
                    if (partInfo.isSingleTable()) {
                        schemaName = partInfo.getTableSchema();

                    }
                }
            }
        } else {

        }
        return "";
    }

    /**
     * 是否仅在一个Group上，此时上层无需分配Union节点
     */
    public boolean isSingleGroup(boolean allowFalseCondition) {

        ExecutionContext ec = PlannerContext.getPlannerContext(this).getExecutionContext();
        TddlRuleManager tddlRuleManager = ec.getSchemaManager(schemaName).getTddlRuleManager();
        if (PlannerUtils.allTableSingle(tableNames, schemaName, tddlRuleManager)) {
            return true;
        }
        if (pushDownOpt.couldDynamicPruning() && pushDownOpt.dynamicPruningContainsPartitionKey()) {
            return false;
        }
        if (tableNames.size() == 1) {
            List<String> shardColumns;
            if (!isNewPartDbTbl()) {
                TableRule tr = tddlRuleManager.getTableRule(getLogicalTableName());
                if (tr == null) {
                    return true;
                }
                if (tr.getExtPartitions() != null && tr.getExtPartitions().size() > 0) {
                    return false;
                }
                shardColumns = tr.getShardColumns();
            } else {
                PartitionInfoManager partitionInfoManager = tddlRuleManager.getPartitionInfoManager();
                PartitionInfo partitionInfo = partitionInfoManager.getPartitionInfo(getLogicalTableName());
                shardColumns = partitionInfo.getPartitionColumns();
            }
            return PlannerUtils.atSingleGroup(shardColumns, getRelShardInfo(ec), allowFalseCondition);
        } else {
            boolean hasOne = false;
            boolean hasMul = false;
            for (int i = 0; i < tableNames.size(); i++) {
                String name = tableNames.get(i);
                List<String> shardColumns;
                if (!isNewPartDbTbl()) {
                    TableRule tr = tddlRuleManager.getTableRule(name);

                    /**
                     * 如果是广播表,跳过
                     */
                    if (tr == null) {
                        if (hasMul) {
                            return false;
                        }
                        hasOne = true;
                        continue;
                    } else if (tr.isBroadcast()) {
                        continue;
                    }

                    if (tr.getExtPartitions() != null && tr.getExtPartitions().size() > 0) {
                        return false;
                    }

                    shardColumns = tr.getShardColumns();
                } else {
                    PartitionInfoManager partitionInfoManager = tddlRuleManager.getPartitionInfoManager();
                    PartitionInfo partitionInfo =
                        partitionInfoManager.getPartitionInfo(getLogicalTableName());
                    shardColumns = partitionInfo.getPartitionColumns();
                }

                if (!PlannerUtils.atSingleGroup(shardColumns, getRelShardInfo(ec), allowFalseCondition)) {
                    return false;
                }
                if (hasMul || hasOne) {
                    return false;
                }
                hasMul = true;
            }
            return true;
        }
    }

    public PushDownOpt getPushDownOpt() {
        return pushDownOpt;
    }

    /**
     * <pre>
     * If exists sharding table whose sharding column is visible to the upper node, return the name of first one
     * else If all table is broadcast table, return getLogicalTableName() which is the first one in the tableNames
     * else return first non-broadcast table name
     * </pre>
     *
     * @return name of representative table for join pushdown
     */
    public String getShardingTable() {
        if (GeneralUtil.isEmpty(this.tableNames)) {
            return "";
        }

        final TddlRuleManager rule =
            PlannerContext.getPlannerContext(this).getExecutionContext().getSchemaManager(schemaName)
                .getTddlRuleManager();
        final List<String> fieldNames = pushDownOpt.getPlainRowType().getFieldNames();

        // use case insensitive set for column name
        final Collector<String, ?, TreeSet<String>> columnCollector = Collectors.toCollection(
            () -> new TreeSet<>(String.CASE_INSENSITIVE_ORDER));

        return getPlainRefIndex()
            .stream()
            .filter(pair -> pair.getKey() >= 0 && pair.getKey() < fieldNames.size())
            .filter(pair -> rule.isShard(pair.getValue()))
            // group by table name
            .collect(
                Collectors.groupingBy(Pair::getValue,
                    // get column name
                    Collectors.mapping(pair -> fieldNames.get(pair.getKey()), columnCollector)))
            .entrySet().stream()
            /*
              If exists sharding table whose sharding column is visible to the upper node, return the name of first one
             */
            .filter(tableColumns -> Optional.ofNullable(rule.getSharedColumns(tableColumns.getKey()))
                .filter(shardColumns -> tableColumns.getValue().containsAll(shardColumns))
                .isPresent())
            .map(Entry::getKey)
            .findFirst()
            /*
             * else If not all table are broadcast table, return first non-broadcast table name
             */
            .orElseGet(() -> this.tableNames
                .stream()
                .filter(tableName -> !rule.isBroadCast(tableName))
                .findFirst()
                /*
                 * else return getLogicalTableName()
                 * which is the first one in the tableNames
                 */
                .orElseGet(this::getLogicalTableName));
    }

    @Override
    public String getSchemaName() {
        return schemaName;
    }

    public List<RexDynamicParam> getScalarList() {
        return scalarList;
    }

    public LogicalView setScalarList(List<RexDynamicParam> scalarList) {
        this.scalarList = scalarList;
        return this;
    }

    @Override
    public void childrenAccept(RelVisitor visitor) {
        visitor.visit(pushDownOpt.getBuilder().peek(), 0, this);
    }

    public boolean isCrossSingleTable() {
        return crossSingleTable;
    }

    public void setCrossSingleTable(boolean crossSingleTable) {
        this.crossSingleTable = crossSingleTable;
    }

    public List<RexFieldAccess> getCorrelateVariableScalar() {
        return correlateVariableScalar;
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        if (ConfigDataMode.isFastMock() || !PlannerContext.getPlannerContext(this).getParamManager()
            .getBoolean(ConnectionParams.ENABLE_LOGICALVIEW_COST)) {
            return DrdsRelOptCostImpl.TINY;
        }
        if (join != null) {
            Index index = getLookupJoin().getLookupIndex();
            RelNode mysqlRelNode = getMysqlNode();
            RelOptCost scanCost = mq.getCumulativeCost(mysqlRelNode);

            final double rows;
            final double cpu;
            final double memory;
            final double io;

            // for bka join, logicalview must contain only one tablescan.
            // make this tablescan io cost like index access
            if (index != null) {
                double selectivity = index.getTotalSelectivity();
                rows = Math.max(table.getRowCount() * selectivity, 1);
                cpu = scanCost.getCpu() * selectivity;
                memory = scanCost.getMemory() * selectivity;
                double size =
                    TableScanIOEstimator.estimateRowSize(table.getRowType()) * table.getRowCount() * selectivity;
                io = Math.ceil(size / CostModelWeight.RAND_IO_PAGE_SIZE);
            } else {
                rows = scanCost.getRows();
                cpu = scanCost.getCpu();
                memory = scanCost.getMemory();
                io = scanCost.getIo();
            }

            RelOptCost indexCost = planner.getCostFactory().makeCost(rows, cpu, memory, io, 0);
            return indexCost;
        }
        // MetaQuery will compute the pushed node cumulative cost
        RelNode mysqlRelNode = getMysqlNode();
        RelOptCost cost = mq.getCumulativeCost(mysqlRelNode);

        double size = TableScanIOEstimator.estimateRowSize(this.getRowType()) * mq.getRowCount(this);
        double net = Math.ceil(size / CostModelWeight.NET_BUFFER_SIZE);

        RelOptCost smallerCost = this.getCluster().getPlanner().getCostFactory()
            .makeCost(cost.getRows(), cost.getCpu() * 0.9, cost.getMemory() * 0.9, cost.getIo(), net);

        int shardUpperBound = calShardUpperBound();
        smallerCost = smallerCost.plus(planner.getCostFactory().makeCost(0, 0, 0, 0,
            (shardUpperBound - 1) * CostModelWeight.INSTANCE.getShardWeight()));

        return smallerCost;
    }

    protected List<String> collectTableNames() {
        final List<String> tables = new ArrayList<>();
        getNativeSqlNode().accept(new ReplaceTableNameWithSomethingVisitor(schemaName,
            PlannerContext.getPlannerContext(this).getExecutionContext()) {
            @Override
            protected SqlNode buildSth(SqlNode sqlNode) {
                if (sqlNode instanceof SqlIdentifier) {
                    tables.add(((SqlIdentifier) sqlNode).getLastName());
                }
                return sqlNode;
            }
        });
        return tables;
    }

    public void rebuildPartRoutingPlanInfo() {
        this.pushDownOpt.rebuildPartRoutingPlanInfo();
    }

    public LockMode getLockMode() {
        return lockMode;
    }

    public void setLockMode(LockMode lockMode) {
        this.lockMode = lockMode;
    }

    public void optimizePhySql() {
        getPushDownOpt().optimizePhySql();
        tableNames = collectTableNames();
        rebuildPartRoutingPlanInfo();
    }

    public void optimize() {
        getPushDownOpt().optimize();
        tableNames = collectTableNames();
        rebuildPartRoutingPlanInfo();
        rebuildOriginColumnNames();
    }

    /**
     * rebuild origin columns info for lookup executor building its LookupPredicate condition.
     */
    private void rebuildOriginColumnNames() {
        if (this.getJoin() != null) {
            Join join = this.getJoin();
            RelMetadataQuery mq = join.getCluster().getMetadataQuery();
            for (int i = 0; i < this.getRowType().getFieldCount(); i++) {
                RelColumnOrigin columnOrigin = mq.getColumnOrigin(this, i);
                if (columnOrigin == null) {
                    columnOrigins.add(this.getRowType().getFieldNames().get(i));
                } else {
                    columnOrigins.add(columnOrigin.getColumnName());
                }
            }
        }
    }

    public List<RelNode> getInput(UnionOptHelper helper, ExecutionContext executionContext,
                                  boolean forceIgnoreRF, SqlSelect sqlTemplate) {
        return getInnerInput(sqlTemplate, helper, executionContext, forceIgnoreRF);
    }

    public List<RelNode> getInput(UnionOptHelper helper, ExecutionContext executionContext,
                                  boolean forceIgnoreRF) {
        return getInnerInput(helper, executionContext, forceIgnoreRF);
    }

    public boolean isSingleGroup() {
        final boolean singleGroup = isSingleGroup(false);
        return singleGroup;
    }

    public String explainNodeName() {
        String name = "LogicalView";
        return name;

    }

    public List<Integer> getBloomFilters() {
        List<Integer> bloomFilterIds = new ArrayList<>();
        if (pushDownOpt != null && pushDownOpt.getPushedRelNode() != null) {
            new RelVisitor() {
                @Override
                public void visit(RelNode node, int ordinal, RelNode parent) {
                    if (node instanceof LogicalFilter) {
                        List<RexNode> conditions = RelOptUtil.conjunctions(
                            ((LogicalFilter) node).getCondition());
                        for (RexNode rexNode : conditions) {
                            if (rexNode instanceof RexCall &&
                                ((RexCall) rexNode).getOperator() instanceof SqlRuntimeFilterFunction) {
                                SqlRuntimeFilterFunction runtimeFilterFunction =
                                    (SqlRuntimeFilterFunction) ((RexCall) rexNode).getOperator();
                                bloomFilterIds.add(runtimeFilterFunction.getId());
                            }

                        }

                    }
                    super.visit(node, ordinal, parent);
                }
            }.go(pushDownOpt.getPushedRelNode());
        }
        return bloomFilterIds;
    }

    /**
     * 仅适用于刚初始化的 LogicalView 对象
     */
    @Override
    public RelNode clone() {
        final LogicalView logicalView = new LogicalView(this, lockMode);
        logicalView.setScalarList(scalarList);
        logicalView.correlateVariableScalar.addAll(correlateVariableScalar);
        logicalView.flashback = this.flashback;
        logicalView.columnOrigins = this.getColumnOrigins();
        return logicalView;
    }

    @Override
    public final RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return clone();
    }

    public LogicalView copy(RelTraitSet traitSet) {
        LogicalView newLogicalView = new LogicalView(this);
        newLogicalView.traitSet = traitSet;
        newLogicalView.newPartDbTbl = this.newPartDbTbl;
        newLogicalView.pushDownOpt = pushDownOpt.copy(newLogicalView, this.getPushedRelNode());
        newLogicalView.flashback = this.flashback;
        newLogicalView.columnOrigins = this.getColumnOrigins();
        return newLogicalView;
    }

    public LogicalView copy(RelTraitSet traitSet, RelNode newPushRelNode) {
        LogicalView newLogicalView = copy(traitSet);
        newLogicalView.pushDownOpt = pushDownOpt.copy(newLogicalView, newPushRelNode);
        newLogicalView.correlateVariableScalar.clear();
        newLogicalView.buildApply();
        return newLogicalView;
    }

    public RelColumnOrigin getTargetColumnOrigin(BaseTableScanFinder tableScanFinder) {
        if (!(tableScanFinder instanceof LogicalView.LookupTableScanFinder)) {
            throw new InvalidParameterException("need LookupTableScanFinder");
        }
        LogicalView.LookupTableScanFinder finder = (LogicalView.LookupTableScanFinder) tableScanFinder;
        if (finder.tableScans == null) {
            return null;
        }
        TableScan best = null;
        Double bestRowCount = 0.0D;
        ColumnMeta bestPkFiled = null;
        for (int i = 0; i < finder.tableScans.size(); i++) {
            final TableScan scan = finder.tableScans.get(i);
            final List<String> qualifiedName = scan.getTable().getQualifiedName();
            String innerSchemaName = getSchemaName();
            String tableName = Util.last(qualifiedName);
            final TddlRuleManager rule =
                PlannerContext.getPlannerContext(this).getExecutionContext().getSchemaManager(schemaName)
                    .getTddlRuleManager();
            if (rule != null) {
                final boolean broadCast = rule.isBroadCast(tableName);
                if (broadCast) {
                    continue;
                }
            }

            ColumnMeta pkFiled = getPkField(innerSchemaName, tableName);
            if (pkFiled == null) {
                continue;
            }

            LogicalView.Node node = finder.getNode(best);
            boolean findAgg = false;
            while (node != null) {
                final RelNode parent = node.getParent();
                if (parent == null) {
                    break;
                }
                if (parent instanceof Aggregate) {
                    if (finder.getNode(parent) != null) {
                        findAgg = true;
                    }
                }
                node = finder.getNode(parent);
            }

            if (findAgg) {
                continue;
            }

            Double currRowCount = RelUtils.getRowCount(scan);
            if (currRowCount > bestRowCount) {
                best = scan;
                bestPkFiled = pkFiled;
                bestRowCount = currRowCount;
            }

        }
        if (best != null) {
            finder.tableName = Util.last(best.getTable().getQualifiedName());
            return new RelColumnOrigin(best.getTable(),
                best.getRowType().getFieldNames().indexOf(bestPkFiled.getName()),
                false);
        }
        return null;
    }

    private ColumnMeta getPkField(String schemaName, String tableName) {
        final TableMeta table =
            PlannerContext.getPlannerContext(this).getExecutionContext().getSchemaManager(schemaName)
                .getTable(tableName);
        final Collection<ColumnMeta> primaryKeys = table.getPrimaryKey();
        if (primaryKeys == null || primaryKeys.size() == 0) {
            return null;
        }
        final ColumnMeta next = primaryKeys.iterator().next();
        return next;
    }

    public boolean isExpandView() {
        return expandView;
    }

    public LogicalView setExpandView(boolean expandView) {
        this.expandView = expandView;
        return this;
    }

    public BaseQueryOperation fromTableOperation() {
        return queryOperation;
    }

    public void setFromTableOperation(BaseQueryOperation queryOperation) {
        this.queryOperation = queryOperation;
    }

    public BytesSql getLookupSqlTemplateCache(Supplier<BytesSql> generator) {
        if (isCacheForbidden()) {
            return generator.get();
        }
        if (lookupSqlTemplateCache == null) {
            lookupSqlTemplateCache = generator.get();
        }
        return lookupSqlTemplateCache;
    }

    public ExtractionResult getPartitionConditionCache(Supplier<ExtractionResult> generator) {
        if (partitionConditionCache == null) {
            partitionConditionCache = generator.get();
        }
        return partitionConditionCache;
    }

    public boolean hasDynamicPruning() {
        return pushDownOpt.couldDynamicPruning();
    }

    public List<String> getColumnOrigins() {
        return columnOrigins;
    }

    public static class ReplacedTableCondition {
        private boolean replaced;
        private RelNode relNode;

        public ReplacedTableCondition(RelNode relNode, boolean replaced) {
            this.relNode = relNode;
            this.replaced = replaced;
        }
    }

    protected boolean checkIfNewPartDbTbl(List<String> tableNames) {
        boolean isNewPartDb = DbInfoManager.getInstance().isNewPartitionDb(schemaName);
        if (isNewPartDb) {
            PartitionInfoManager partInfoMgr =
                PlannerContext.getPlannerContext(this).getExecutionContext().getSchemaManager(schemaName)
                    .getTddlRuleManager()
                    .getPartitionInfoManager();

            boolean isJoin = tableNames.size() > 1;
            for (int i = 0; i < tableNames.size(); i++) {
                String tb = tableNames.get(i);
                PartitionInfo partInfo = partInfoMgr.getPartitionInfo(tb);
                validateSelectedPartitions(true, partInfo, isJoin);
            }
            return true;
        } else {
            boolean isJoin = tableNames.size() > 1;
            validateSelectedPartitions(false, null, isJoin);
            return false;
        }
    }

    static public class DynamicRexReplacer extends RexShuttle {
        private RexBuilder rexBuilder;
        private List<Object> objects;
        private boolean replaced;

        protected DynamicRexReplacer(List<Object> objects, RexBuilder rexBuilder) {
            this.rexBuilder = rexBuilder;
            this.objects = objects;
        }

        public boolean isReplaced() {
            return replaced;
        }

        @Override
        public RexNode visitDynamicParam(RexDynamicParam dynamicParam) {
            if (dynamicParam.getDynamicType() == RexDynamicParam.DYNAMIC_TYPE_VALUE.SINGLE_PARALLEL) {
                final RelDataType type = dynamicParam.getType();
                final int index = dynamicParam.getIndex();
                final RexLiteral rexLiteral = makeLiteral(rexBuilder, type, objects.get(index));
                replaced = true;
                return rexLiteral;
            }
            return dynamicParam;

        }

    }

    abstract class BaseTableScanFinder extends RelShuttleImpl {
        protected String tableName;
        protected LogicalView.NodeFinder nodeFinder = new LogicalView.NodeFinder();

        protected LogicalView.Node getNode(RelNode rel) {
            LogicalView.Node node = nodeFinder.get(rel);
            if (node == null) {
                node = new LogicalView.Node(rel);
                nodeFinder.put(rel, node);
            }
            return node;
        }

        @Override
        public RelNode visit(LogicalAggregate aggregate) {
            final RelNode visit = super.visit(aggregate);
            return visit;
        }

        @Override
        protected RelNode visitChildren(RelNode rel) {
            LogicalView.Node node = getNode(rel);
            for (Ord<RelNode> input : Ord.zip(rel.getInputs())) {
                LogicalView.Node cNode = getNode(input.e);
                cNode.setParent(rel);
                node.addChild(input.e);
                rel = visitChild(rel, input.i, input.e);
            }
            return rel;
        }

        public abstract boolean is();

        public String getTableName() {
            return tableName;
        }

    }

    class LookupTableScanFinder extends BaseTableScanFinder {
        private boolean isFound;
        private List<TableScan> tableScans = new ArrayList<>();

        @Override
        public RelNode visit(TableScan scan) {
            tableScans.add(scan);
            LogicalView.Node node = getNode(scan);
            String tableScanStrings = "";
            for (int i = 0; i < tableScans.size(); i++) {
                tableScanStrings = tableScanStrings + "   \n" + tableScans.get(i).getDescription();
            }
            if (logger.isDebugEnabled()) {
                logger.debug("tableScans: " + tableScanStrings + "\nand add new node:" + node.toString());
            }
            return scan;
        }

        @Override
        public RelNode visit(LogicalJoin join) {
            RelNode rel = visitChildren(join);
            return rel;
        }

        @Override
        public RelNode visit(LogicalAggregate aggregate) {
            final RelNode visit = super.visit(aggregate);
            return visit;
        }

        @Override
        protected RelNode visitChildren(RelNode rel) {
            return super.visitChildren(rel);
        }

        @Override
        public boolean is() {
            final RelColumnOrigin targetColumnOrigin = getTargetColumnOrigin(this);
            if (targetColumnOrigin != null) {
                isFound = true;
            }
            return isFound;
        }

    }

    public class NodeFinder {

        private Map<RelNode, LogicalView.Node> map = new HashMap<>();

        public void put(RelNode currentRelNode, LogicalView.Node node) {
            map.put(currentRelNode, node);
        }

        public LogicalView.Node get(RelNode relNode) {
            return map.get(relNode);
        }

    }

    public class Node {
        private RelNode parent;
        private RelNode currentRelNode;
        private List<RelNode> children;

        public Node(RelNode relNode) {
            this.currentRelNode = relNode;
        }

        public void addChild(RelNode child) {
            if (this.children == null) {
                children = new ArrayList<>();
            }
            children.add(child);
        }

        public RelNode getParent() {
            return parent;
        }

        public void setParent(RelNode parent) {
            this.parent = parent;
        }

        public RelNode getCurrentRelNode() {
            return currentRelNode;
        }

        public List<RelNode> getChildren() {
            return children;
        }

        @Override
        public String toString() {
            String childrenString = "";
            if (children != null) {
                for (int i = 0; i < children.size(); i++) {
                    childrenString = childrenString + "   \n" + children.get(i).getDescription();
                }
            }
            return "parent:" + RelOptUtil.toString(parent) + ",currentRelNode" +
                ":" + getCurrentRelNode().getDescription() + "\nchildren:" + childrenString;
        }
    }

    public boolean isNewPartDbTbl() {
        return newPartDbTbl;
    }

    public RelFieldCollation.Direction collationMatchRangePartition() {
        PartitionInfo partitionInfo = PlannerContext.getPlannerContext(this).getExecutionContext()
            .getSchemaManager(this.getSchemaName()).getTddlRuleManager().getPartitionInfoManager()
            .getPartitionInfo(this.getShardingTable());

        final RelCollation collation;
        RelNode pushedRelNode = this.getPushedRelNode();
        if (pushedRelNode instanceof Sort) {
            collation = ((Sort) pushedRelNode).getCollation();
        } else {
            return null;
        }

        if (collation.isTop()) {
            return null;
        }

        if (partitionInfo == null) {
            return null;
        }

        RelMetadataQuery mq = this.getCluster().getMetadataQuery();

        // FIXME: subPartition
        if (partitionInfo.getPartitionBy() == null || partitionInfo.getPartitionBy().getSubPartitionBy() != null) {
            return null;
        }

        final List<String> partitionColumnNameList;
        switch (partitionInfo.getPartitionBy().getStrategy()) {
        case RANGE:
        case RANGE_COLUMNS:
            partitionColumnNameList = partitionInfo.getPartitionBy().getPartitionColumnNameList();
            break;
        default:
            return null;
        }

        // collation same direction
        RelFieldCollation.Direction direction = null;
        for (RelFieldCollation fieldCollation : collation.getFieldCollations()) {
            if (direction == null && (
                fieldCollation.getDirection() == RelFieldCollation.Direction.DESCENDING
                    || fieldCollation.getDirection() == RelFieldCollation.Direction.ASCENDING)) {
                direction = fieldCollation.getDirection();
            } else if (direction != fieldCollation.getDirection()) {
                return null;
            }
        }

        List<String> sortColumnNameList = new ArrayList<>();
        // can find column origin
        for (RelFieldCollation fieldCollation : collation.getFieldCollations()) {
            RelColumnOrigin columnOrigin = mq.getColumnOrigin(this, fieldCollation.getFieldIndex());
            if (columnOrigin == null) {
                return null;
            }
            // must ref sharding table
            // TODO: join column equality
            if (!this.getShardingTable()
                .equalsIgnoreCase(CBOUtil.getTableMeta(columnOrigin.getOriginTable()).getTableName())) {
                return null;
            }
            sortColumnNameList.add(columnOrigin.getColumnName());
        }

        // sort column should be prefix of range partition column
        for (int i = 0; i < sortColumnNameList.size(); i++) {
            if (i >= partitionColumnNameList.size()) {
                return null;
            }
            if (!sortColumnNameList.get(i).equalsIgnoreCase(partitionColumnNameList.get(i))) {
                return null;
            }
        }

        return direction;
    }

    public RelOptCost getSelfCost(RelMetadataQuery mq) {
        if (selfCost != null) {
            return selfCost;
        }
        return selfCost = computeSelfCost(getCluster().getPlanner(), mq);
    }

    public synchronized Double getRowCount(RelMetadataQuery mq) {
        return mq.getRowCount(getOptimizedPushedRelNodeForMetaQuery());
    }

    public synchronized Set<RelColumnOrigin> getColumnOrigins(RelMetadataQuery mq, int iOutputColumn) {
        return mq.getColumnOrigins(getPushedRelNode(), iOutputColumn);
    }

    public synchronized Double getDistinctRowCount(RelMetadataQuery mq,
                                                   ImmutableBitSet groupKey, RexNode predicate) {
        return mq.getDistinctRowCount(getOptimizedPushedRelNodeForMetaQuery(), groupKey, predicate);
    }

    public synchronized Double getSelectivity(RelMetadataQuery mq, RexNode predicate) {
        return mq.getSelectivity(getOptimizedPushedRelNodeForMetaQuery(), predicate);
    }

    public synchronized Set<RexTableInputRef.RelTableRef> getTableReferences(RelMetadataQuery mq,
                                                                             boolean logicalViewLevel) {
        if (logicalViewLevel) {
            return Sets.newHashSet(RexTableInputRef.RelTableRef.of(getTable(), 0));
        } else {
            return mq.getTableReferences(getPushedRelNode());
        }
    }

    public synchronized Boolean areColumnsUnique(RelMetadataQuery mq, ImmutableBitSet columns, boolean ignoreNulls) {
        return mq.areColumnsUnique(getPushedRelNode(), columns, ignoreNulls);
    }

    public synchronized List<Set<RelColumnOrigin>> isCoveringIndex(RelMetadataQuery mq, RelOptTable table,
                                                                   String index) {
        return mq.isCoveringIndex(getPushedRelNode(), table, index);
    }

    public synchronized RelOptPredicateList getPredicates(RelMetadataQuery mq) {
        return mq.getPulledUpPredicates(getOptimizedPushedRelNodeForMetaQuery());
    }

    public synchronized Double getPopulationSize(RelMetadataQuery mq, ImmutableBitSet groupKey) {
        return mq.getPopulationSize(getOptimizedPushedRelNodeForMetaQuery(), groupKey);
    }

    public Map<ImmutableBitSet, ImmutableBitSet> getFunctionalDependency(RelMetadataQuery mq,
                                                                         ImmutableBitSet iOutputColumns) {
        return mq.getFunctionalDependency(getPushedRelNode(), iOutputColumns);
    }

    public boolean useSelectPartitions() {
        if (this.partitions != null) {
            SqlNodeList partNamesAst = (SqlNodeList) this.partitions;
            if (partNamesAst.size() > 0) {
                return true;
            }
        }
        return false;
    }

    public boolean isFromMergeIndex() {
        return fromMergeIndex;
    }

    public void setFromMergeIndex(boolean fromMergeIndex) {
        this.fromMergeIndex = fromMergeIndex;
    }

    /**
     * 一些特殊情况select 不可直接下推标识：
     * 1、as of tso [expr], expr包含不确定性表达式, 需上层计算
     */
    public boolean unPushDown() {
        if (getFlashback() != null) {
            return !RexUtil.isDeterministic(getFlashback());
        }
        return false;
    }
}
