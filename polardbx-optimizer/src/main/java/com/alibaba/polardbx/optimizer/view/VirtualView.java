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

package com.alibaba.polardbx.optimizer.view;

import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author dylan
 */
public class VirtualView extends AbstractRelNode {

    private VirtualViewType virtualViewType;

    // column -> values(RexDynamicParam or RexLiteral)
    // such as schemaName & tableName for information_schema.tables
    private Map<Integer, List<Object>> index = new HashMap<>();

    private Map<Integer, Object> like = new HashMap<>();

    protected VirtualView(RelOptCluster cluster, RelTraitSet traitSet, VirtualViewType virtualViewType) {
        super(cluster, traitSet);
        this.virtualViewType = virtualViewType;
    }

    /**
     * Constructor for JSON deserialization
     */
    public VirtualView(RelInput relInput) {
        this(relInput.getCluster(), relInput.getTraitSet(),
            relInput.getEnum("virtualViewType", VirtualViewType.class));
        this.traitSet = this.traitSet.replace(DrdsConvention.INSTANCE);
    }

    public static VirtualView create(RelOptCluster cluster, VirtualViewType type) {
        return create(cluster, cluster.getPlanner().emptyTraitSet(), type);
    }

    public static VirtualView create(RelOptCluster cluster, RelTraitSet traitSet, VirtualViewType type) {
        switch (type) {
        case VIRTUAL_STATISTIC:
            return new VirtualStatistic(cluster, traitSet);
        case INFORMATION_SCHEMA_TABLES:
            return new InformationSchemaInformationSchemaTables(cluster, traitSet);
        case INFORMATION_SCHEMA_COLUMNS:
            return new InformationSchemaInformationSchemaColumns(cluster, traitSet);
        case STATISTICS:
            return new InformationSchemaStatistics(cluster, traitSet);
        case SCHEMATA:
            return new InformationSchemaSchemata(cluster, traitSet);
        case TABLES:
            return new InformationSchemaTables(cluster, traitSet);
        case COLUMNS:
            return new InformationSchemaColumns(cluster, traitSet);
        case COLUMN_STATISTICS:
            return new InformationSchemaColumnStatistics(cluster, traitSet);
        case ENGINES:
            return new InformationSchemaEngines(cluster, traitSet);
        case KEYWORDS:
            return new InformationSchemaKeywords(cluster, traitSet);
        case COLLATIONS:
            return new InformationSchemaCollations(cluster, traitSet);
        case CHARACTER_SETS:
            return new InformationSchemaCharacterSets(cluster, traitSet);
        case COLLATION_CHARACTER_SET_APPLICABILITY:
            return new InformationSchemaCollationCharacterSetApplicability(cluster, traitSet);
        case INNODB_SYS_DATAFILES:
            return new InformationSchemaInnodbSysDatafiles(cluster, traitSet);
        case INNODB_SYS_TABLES:
            return new InformationSchemaInnodbSysTables(cluster, traitSet);
        case TABLE_CONSTRAINTS:
            return new InformationSchemaTableConstraints(cluster, traitSet);
        case EVENTS:
            return new InformationSchemaEvents(cluster, traitSet);
        case TRIGGERS:
            return new InformationSchemaTriggers(cluster, traitSet);
        case ROUTINES:
            return new InformationSchemaRoutines(cluster, traitSet);
        case COLUMN_PRIVILEGES:
            return new InformationSchemaColumnPrivileges(cluster, traitSet);
        case FILES:
            return new InformationSchemaFiles(cluster, traitSet);
        case GLOBAL_STATUS:
            return new InformationSchemaGlobalStatus(cluster, traitSet);
        case GLOBAL_VARIABLES:
            return new InformationSchemaGlobalVariables(cluster, traitSet);
        case KEY_COLUMN_USAGE:
            return new InformationSchemaKeyColumnUsage(cluster, traitSet);
        case OPTIMIZER_TRACE:
            return new InformationSchemaOptimizerTrace(cluster, traitSet);
        case PARAMETERS:
            return new InformationSchemaParameters(cluster, traitSet);
        case PARTITIONS:
            return new InformationSchemaPartitions(cluster, traitSet);
        case PLUGINS:
            return new InformationSchemaPlugins(cluster, traitSet);
        case PROCESSLIST:
            return new InformationSchemaProcesslist(cluster, traitSet);
        case PROFILING:
            return new InformationSchemaProfiling(cluster, traitSet);
        case REFERENTIAL_CONSTRAINTS:
            return new InformationSchemaReferentialConstraints(cluster, traitSet);
        case SCHEMA_PRIVILEGES:
            return new InformationSchemaSchemaPrivileges(cluster, traitSet);
        case SESSION_STATUS:
            return new InformationSchemaSessionStatus(cluster, traitSet);
        case SESSION_VARIABLES:
            return new InformationSchemaSessionVariables(cluster, traitSet);
        case TABLESPACES:
            return new InformationSchemaTablespaces(cluster, traitSet);
        case TABLE_PRIVILEGES:
            return new InformationSchemaTablePrivileges(cluster, traitSet);
        case USER_PRIVILEGES:
            return new InformationSchemaUserPrivileges(cluster, traitSet);
        case INNODB_LOCKS:
            return new InformationSchemaInnodbLocks(cluster, traitSet);
        case INNODB_TRX:
            return new InformationSchemaInnodbTrx(cluster, traitSet);
        case INNODB_FT_CONFIG:
            return new InformationSchemaInnodbFtConfig(cluster, traitSet);
        case INNODB_SYS_VIRTUAL:
            return new InformationSchemaInnodbSysVirtual(cluster, traitSet);
        case INNODB_CMP:
            return new InformationSchemaInnodbCmp(cluster, traitSet);
        case INNODB_FT_BEING_DELETED:
            return new InformationSchemaInnodbFtBeingDeleted(cluster, traitSet);
        case INNODB_CMP_RESET:
            return new InformationSchemaInnodbCmpReset(cluster, traitSet);
        case INNODB_CMP_PER_INDEX:
            return new InformationSchemaInnodbCmpPerIndex(cluster, traitSet);
        case INNODB_CMPMEM_RESET:
            return new InformationSchemaInnodbCmpmemReset(cluster, traitSet);
        case INNODB_FT_DELETED:
            return new InformationSchemaInnodbFtDeleted(cluster, traitSet);
        case INNODB_BUFFER_PAGE_LRU:
            return new InformationSchemaInnodbBufferPageLru(cluster, traitSet);
        case INNODB_LOCK_WAITS:
            return new InformationSchemaInnodbLockWaits(cluster, traitSet);
        case INNODB_TEMP_TABLE_INFO:
            return new InformationSchemaInnodbTempTableInfo(cluster, traitSet);
        case INNODB_SYS_INDEXES:
            return new InformationSchemaInnodbSysIndexes(cluster, traitSet);
        case INNODB_SYS_FIELDS:
            return new InformationSchemaInnodbSysFields(cluster, traitSet);
        case INNODB_CMP_PER_INDEX_RESET:
            return new InformationSchemaInnodbCmpPerIndexReset(cluster, traitSet);
        case INNODB_BUFFER_PAGE:
            return new InformationSchemaInnodbBufferPage(cluster, traitSet);
        case INNODB_FT_DEFAULT_STOPWORD:
            return new InformationSchemaInnodbFtDefaultStopword(cluster, traitSet);
        case INNODB_FT_INDEX_TABLE:
            return new InformationSchemaInnodbFtIndexTable(cluster, traitSet);
        case INNODB_FT_INDEX_CACHE:
            return new InformationSchemaInnodbFtIndexCache(cluster, traitSet);
        case INNODB_SYS_TABLESPACES:
            return new InformationSchemaInnodbSysTablespaces(cluster, traitSet);
        case INNODB_METRICS:
            return new InformationSchemaInnodbMetrics(cluster, traitSet);
        case INNODB_SYS_FOREIGN_COLS:
            return new InformationSchemaInnodbSysForeignCols(cluster, traitSet);
        case INNODB_CMPMEM:
            return new InformationSchemaInnodbCmpmem(cluster, traitSet);
        case INNODB_BUFFER_POOL_STATS:
            return new InformationSchemaInnodbBufferPoolStats(cluster, traitSet);
        case INNODB_SYS_COLUMNS:
            return new InformationSchemaInnodbSysColumns(cluster, traitSet);
        case INNODB_SYS_FOREIGN:
            return new InformationSchemaInnodbSysForeign(cluster, traitSet);
        case INNODB_SYS_TABLESTATS:
            return new InformationSchemaInnodbSysTablestats(cluster, traitSet);
        case DRDS_PHYSICAL_PROCESS_IN_TRX:
            return new InformationSchemaDrdsPhysicalProcessInTrx(cluster, traitSet);
        case WORKLOAD:
            return new InformationSchemaWorkload(cluster, traitSet);
        case QUERY_INFO:
            return new InformationSchemaQueryInfo(cluster, traitSet);
        case GLOBAL_INDEXES:
            return new InformationSchemaGlobalIndexes(cluster, traitSet);
        case METADATA_LOCK:
            return new InformationSchemaMetadataLock(cluster, traitSet);
        case TABLE_GROUP:
            return new InformationSchemaTableGroup(cluster, traitSet);
        case TABLE_DETAIL:
            return new InformationSchemaTableDetail(cluster, traitSet);
        case LOCALITY_INFO:
            return new InformationSchemaLocalityInfo(cluster, traitSet);
        case MOVE_DATABASE:
            return new InformationSchemaMoveDatabase(cluster, traitSet);
        case PHYSICAL_PROCESSLIST:
            return new InformationSchemaPhysicalProcesslist(cluster, traitSet);
        case PLAN_CACHE:
            return new InformationSchemaPlanCache(cluster, traitSet);
        case STATISTIC_TASK:
            return new InformationSchemaStatisticTask(cluster, traitSet);
        case CCL_RULE:
            return new InformationSchemaCclRules(cluster, traitSet);
        case CCL_TRIGGER:
            return new InformationSchemaCclTriggers(cluster, traitSet);
        case SPM:
            return new InformationSchemaSPM(cluster, traitSet);
        case PLAN_CACHE_CAPACITY:
            return new InformationSchemaPlanCacheCapacity(cluster, traitSet);
        case REACTOR_PERF:
            return new InformationSchemaReactorPerf(cluster, traitSet);
        case DN_PERF:
            return new InformationSchemaDnPerf(cluster, traitSet);
        case TCP_PERF:
            return new InformationSchemaTcpPerf(cluster, traitSet);
        case SESSION_PERF:
            return new InformationSchemaSessionPerf(cluster, traitSet);
        default:
            throw new AssertionError();
        }
    }

    public VirtualView copy(RelTraitSet traitSet) {
        VirtualView newVirtualView = create(this.getCluster(), traitSet, this.virtualViewType);
        newVirtualView.index = new HashMap<>();
        newVirtualView.index.putAll(this.index);
        newVirtualView.like = new HashMap<>();
        newVirtualView.like.putAll(this.like);
        return newVirtualView;
    }

    public VirtualViewType getVirtualViewType() {
        return virtualViewType;
    }

    @Override
    public RelWriter explainTermsForDisplay(RelWriter pw) {
        pw.item(RelDrdsWriter.REL_NAME, "VirtualView");
        pw.item("virtualViewType", virtualViewType.toString());
        pw.itemIf("index", String.join(",",
            index.keySet().stream()
                .map(x -> getRowType().getFieldList().get(x).getName()).collect(Collectors.toList())),
            !index.isEmpty());
        pw.itemIf("like", String.join(",",
            like.keySet().stream()
                .map(x -> getRowType().getFieldList().get(x).getName()).collect(Collectors.toList())),
            !like.isEmpty());
        return pw;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        pw.item(RelDrdsWriter.REL_NAME, "VirtualView");
        pw.item("virtualViewType", virtualViewType.toString());
        return pw;
    }

    public Map<Integer, List<Object>> getIndex() {
        return index;
    }

    public Map<Integer, Object> getLike() {
        return like;
    }

    boolean indexableColumn(int i) {
        return false;
    }

    private void addIndexItem(int i, Object o) {
        List<Object> value = index.get(i);
        if (value == null) {
            value = new ArrayList<>();
            index.put(i, value);
        }

        if (!value.contains(o)) {
            value.add(o);
        }
    }

    private void addLikeItem(int i, Object o) {
        like.put(i, o);
    }

    public void pushFilter(RexNode condition) {
        if (condition == null) {
            return;
        }

        if (condition instanceof RexCall) {
            final RexCall currentCondition = (RexCall) condition;
            switch (currentCondition.getKind()) {
            case EQUALS: {
                if (currentCondition.getOperands().size() == 2) {
                    RexNode operand1 = currentCondition.getOperands().get(0);
                    RexNode operand2 = currentCondition.getOperands().get(1);
                    if (operand1 instanceof RexInputRef) {
                        int indexOp1 = ((RexInputRef) operand1).getIndex();
                        if (indexableColumn(indexOp1)) {
                            if (operand2 instanceof RexDynamicParam) {
                                addIndexItem(indexOp1, operand2);
                            } else if (operand2 instanceof RexLiteral) {
                                addIndexItem(indexOp1, operand2);
                            }
                        }
                    } else if (operand2 instanceof RexInputRef) {
                        int indexOp2 = ((RexInputRef) operand2).getIndex();
                        if (indexableColumn(indexOp2)) {
                            if (operand1 instanceof RexDynamicParam) {
                                addIndexItem(indexOp2, operand1);
                            } else if (operand1 instanceof RexLiteral) {
                                addIndexItem(indexOp2, operand1);
                            }
                        }
                    }
                }
                break;
            }
            case IN: {
                if (currentCondition.getOperands().size() == 2) {
                    RexNode operand1 = currentCondition.getOperands().get(0);
                    RexNode operand2 = currentCondition.getOperands().get(1);
                    if (operand1 instanceof RexInputRef && operand2 instanceof RexCall && operand2
                        .isA(SqlKind.ROW)) {
                        int indexOp1 = ((RexInputRef) operand1).getIndex();
                        if (indexableColumn(indexOp1)) {
                            for (RexNode inValue : ((RexCall) operand2).getOperands()) {
                                if (inValue instanceof RexDynamicParam) {
                                    addIndexItem(indexOp1, inValue);
                                } else if (inValue instanceof RexLiteral) {
                                    addIndexItem(indexOp1, inValue);
                                }
                            }
                        }
                    }

                }
                break;
            }
            case LIKE: {
                if (currentCondition.getOperands().size() == 2) {
                    RexNode operand1 = currentCondition.getOperands().get(0);
                    RexNode operand2 = currentCondition.getOperands().get(1);
                    if (operand1 instanceof RexInputRef) {
                        int indexOp1 = ((RexInputRef) operand1).getIndex();
                        if (indexableColumn(indexOp1)) {
                            if (operand2 instanceof RexDynamicParam) {
                                addLikeItem(indexOp1, operand2);
                            } else if (operand2 instanceof RexLiteral) {
                                addLikeItem(indexOp1, operand2);
                            }
                        }
                    }
                }
                break;
            }
            case AND: {
                for (RexNode rex : currentCondition.getOperands()) {
                    pushFilter(rex);
                }
            }
            default: {
                return;
            }
            }
        }

    }
}
