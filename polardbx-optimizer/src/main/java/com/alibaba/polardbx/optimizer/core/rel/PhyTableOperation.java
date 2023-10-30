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

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.jdbc.TableName;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.druid.util.HexBin;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.CursorMeta;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.googlecode.protobuf.format.JsonFormat;
import com.mysql.cj.x.protobuf.PolarxExecPlan;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.externalize.RelDrdsJsonWriter;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.commons.collections.MapUtils;
import org.openjdk.jol.info.ClassLayout;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 记录查询具体物理库所需的信息，可能查询单表，也可能查询多表
 *
 * @author lingce.ldm 2017-07-13 10:21
 */
public final class PhyTableOperation extends BaseTableOperation {

    public static long INSTANCE_MEM_SIZE = ClassLayout.parseClass(PhyTableOperation.class)
        .instanceSize();

    /**
     * List<String>: the physical table set used by one table partition sql(such join, e.g)
     * The list of List<String>: the table partition list of physical table set
     */
    private List<List<String>> tableNames;
    private Map<Integer, ParameterContext> param;
    private List<Map<Integer, ParameterContext>> batchParameters;
    protected PhyOperationBuilderCommon phyOperationBuilder;
    // memory cost when executing query on physical db, count in byte
    private long execMemCost = 0;
    // The list of logical table
    protected List<String> logicalTableNames;
    protected MemoryAllocatorCtx memoryAllocator;

    /**
     * Label if there is only on PhyTableOperation and go to pushdown by PostPlanner after pruning LogicalView
     * if onlyOnePartitionAfterPruning=true, means
     * In PostPlanner, LogicalView has only one partition of PhyTableOperation left after pruning,
     * and it will go to SingleTableScanHandler/SingleTableModifyHandler,
     * instead of going to LogicalViewHandler to its execution.
     */
    protected boolean onlyOnePartitionAfterPruning = false;

    /**
     * NOT allowed to use new PhyTableOperation() build PhyTableOp,
     * and all the PhyTableOperation building must use PhyTableOperationFactory.getInstance().buildPhyTblOpByParams(PhyTableOpBuildParams params)
     * <pre>
     *     所有的 PhyTableOperation 的生成操作必须走以下接口
     *          PhyTableOperationFactory.getInstance().buildPhyTblOpByParams(PhyTableOpBuildParams params)，
     *     因为 PhyTableOperation 需要以下的五元组来获取事务的物理写连接ID,缺一不可
     *     (logDb/logTbl/phyDb/phyTbl/lockMode),
     *     走 PhyTableOperationFactory.getInstance().buildPhyTblOpByParams(...) 可以保证统一填充这几项的必须信息，
     *     因为对于Auto库，PhyOp 需要依赖上述几项信息获取正确的物理分库连接ID
     * </pre>
     */
    PhyTableOperation(RelOptCluster cluster, RelTraitSet traitSet, RelDataType rowType, CursorMeta cursorMeta,
                      RelNode logicalPlan) {
        super(cluster, traitSet, rowType, cursorMeta, logicalPlan);
    }

    public void setRowType(RelDataType rowType) {
        this.rowType = rowType;
    }

    @Override
    public void setDbIndex(String dbIndex) {
        this.dbIndex = dbIndex;
    }

    @Override
    public void initOperation() {
    }

    @Override
    public List<List<String>> getTableNames() {
        return tableNames;
    }

    public void setTableNames(List<List<String>> tableNames) {
        this.tableNames = tableNames;
    }

    public void setParam(Map<Integer, ParameterContext> param) {
        this.param = param;
    }

    public Map<Integer, ParameterContext> getParam() {
        return param;
    }

    @Override
    public List<Map<Integer, ParameterContext>> getBatchParameters() {
        return batchParameters;
    }

    public void setBatchParameters(List<Map<Integer, ParameterContext>> batchParameters) {
        this.batchParameters = batchParameters;
    }

    @Override
    public Pair<String, Map<Integer, ParameterContext>> getDbIndexAndParam(Map<Integer, ParameterContext> param,
                                                                           List<List<String>> phyTableNamesOutput,
                                                                           ExecutionContext executionContext) {

        if (phyTableNamesOutput != null) {
            phyTableNamesOutput.addAll(this.tableNames);
        }

        /**
         * 由于 PhyTableOperation 的物理SQL对应的参数（即this.param）
         * 全部会在
         *  PhyTableScanBuilder 或 PhyTableScanBuilderForMpp 或 PhyTableModifyBuilder
         *  中计算完成，所以这里就不再需要依赖逻辑SQL级别的参数化参数（即传入参数 param）进行重新计算
         */
        return new Pair<>(dbIndex, this.param);
    }

    @Override
    public Pair<String, Map<Integer, ParameterContext>> getDbIndexAndParam(Map<Integer, ParameterContext> param,
                                                                           ExecutionContext executionContext) {
        return getDbIndexAndParam(param, null, executionContext);
    }

    @Override
    public String getExplainName() {
        return "PhyTableOperation";
    }

    @Override
    protected ExplainInfo buildExplainInfo(Map<Integer, ParameterContext> params, ExecutionContext executionContext) {
        return new ExplainInfo(tableNames, dbIndex, this.param);
    }

    @Override
    public RelWriter explainTermsForDisplay(RelWriter pw) {
        ExplainInfo explainInfo = buildExplainInfo(((RelDrdsWriter) pw).getParams(),
            (ExecutionContext) ((RelDrdsWriter) pw).getExecutionContext());
        pw.item(RelDrdsWriter.REL_NAME, getExplainName());

        boolean usePartTable = false;
        List<PartitionInfo> partInfos = new ArrayList<>();
        if (logicalTableNames != null) {
            for (int i = 0; i < logicalTableNames.size(); i++) {
                String logTb = logicalTableNames.get(i);
                PartitionInfo partInfo =
                    OptimizerContext.getContext(getSchemaName()).getRuleManager().getPartitionInfoManager()
                        .getPartitionInfo(logTb);
                if (partInfo != null) {
                    usePartTable = true;
                }
                partInfos.add(partInfo);
            }
        }

        if (!usePartTable) {
            String groupAndTableName = explainInfo.groupName;
            if (explainInfo.tableNames != null && explainInfo.tableNames.size() > 0) {
                groupAndTableName += (TStringUtil.isNotBlank(explainInfo.groupName) ? "." : "")
                    + TStringUtil.join(explainInfo.tableNames, ",");
                pw.itemIf("tables", groupAndTableName, groupAndTableName != null);
            } else {
                pw.itemIf("groups", groupAndTableName, groupAndTableName != null);
            }
        } else {
            String phyPartNameList = "";
            String phyGrp = explainInfo.groupName;
            List<String> logPhyTbInfos = new ArrayList<>();
            for (int i = 0; i < logicalTableNames.size(); i++) {
                String logPhyTbInfo = "";
                for (int phyDbIdx = 0; phyDbIdx < explainInfo.tableNames.size(); phyDbIdx++) {
                    List<String> phyTbListInSameIdx = (List) explainInfo.tableNames.get(phyDbIdx);
                    String phyTb = phyTbListInSameIdx.get(i);
                    String pName = partInfos.get(i).getPartitionNameByPhyLocation(phyGrp, phyTb);
                    if (phyDbIdx > 0) {
                        logPhyTbInfo += ",";
                    }
                    logPhyTbInfo += pName;
                }
                logPhyTbInfos.add(logPhyTbInfo);
            }

            for (int i = 0; i < logicalTableNames.size(); i++) {
                String logTb = logicalTableNames.get(i);
                String phyTbInfoOfLogTb = logPhyTbInfos.get(i);
                if (i > 0) {
                    phyPartNameList += ",";
                }
                phyPartNameList += String.format("%s[%s]", logTb, phyTbInfoOfLogTb);
            }

            pw.itemIf("tables", phyPartNameList, phyPartNameList != null);
        }

        String sql = TStringUtil.replace(getNativeSql(), "\n", " ");
        pw.item("sql", sql);
        StringBuilder builder = new StringBuilder();
        if (MapUtils.isNotEmpty(explainInfo.params)) {
            String operator = "";
            for (Object c : explainInfo.params.values()) {
                Object v = ((ParameterContext) c).getValue();
                builder.append(operator);
                if (v instanceof TableName) {
                    builder.append(((TableName) v).getTableName());
                } else {
                    String valStr;
                    if (v == null) {
                        valStr = "NULL";
                    } else {
                        valStr = v.toString();
                        if (v instanceof byte[]) {
                            valStr = "0x" + HexBin.encode((byte[]) v);
                        }
                    }
                    builder.append(valStr);
                }
                operator = ",";
            }
            pw.item("params", builder.toString());
        }
        // XPlan explain.
        final ExecutionContext executionContext;
        if (pw instanceof RelDrdsWriter) {
            executionContext = (ExecutionContext) ((RelDrdsWriter) pw).getExecutionContext();
        } else if (pw instanceof RelDrdsJsonWriter) {
            executionContext = (ExecutionContext) ((RelDrdsJsonWriter) pw).getExecutionContext();
        } else {
            executionContext = null;
        }
        if (XTemplate != null && executionContext != null &&
            executionContext.getParamManager().getBoolean(ConnectionParams.EXPLAIN_X_PLAN)) {
            final JsonFormat format = new JsonFormat();
            final PolarxExecPlan.ExecPlan plan = XTemplate.explain(executionContext);
            if (null == plan) {
                pw.item("XPlan", "Denied by param.");
            } else {
                pw.item("XPlan", format.printToString(plan));
            }
        }
        //pw.done(this);
        return pw;
    }

    @Override
    public String toString() {
        RelDrdsWriter writer = new RelDrdsWriter();
        explainTermsForDisplay(writer);
        return super.toString() + String.format("@[%s]", writer.asString());
    }

    public long getExecMemCost() {
        return execMemCost;
    }

    public void setExecMemCost(long execMemCost) {
        this.execMemCost = execMemCost;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        PhyTableOperation phyTableOperation = PhyTableOperationFactory.getInstance().copyFrom(this);
        return phyTableOperation;
    }

    public MemoryAllocatorCtx getMemoryAllocator() {
        return memoryAllocator;
    }

    public void setMemoryAllocator(MemoryAllocatorCtx memoryAllocator) {
        this.memoryAllocator = memoryAllocator;
    }

    public PhyOperationBuilderCommon getPhyOperationBuilder() {
        return phyOperationBuilder;
    }

    public void setPhyOperationBuilder(PhyOperationBuilderCommon phyOperationBuilder) {
        this.phyOperationBuilder = phyOperationBuilder;
    }

    @Override
    public List<String> getLogicalTableNames() {
        return logicalTableNames;
    }

    public void setLogicalTableNames(List<String> logicalTableName) {
        this.logicalTableNames = logicalTableName;
    }

    public boolean isOnlyOnePartitionAfterPruning() {
        return onlyOnePartitionAfterPruning;
    }

    public void setOnlyOnePartitionAfterPruning(boolean onlyOnePartitionAfterPruning) {
        this.onlyOnePartitionAfterPruning = onlyOnePartitionAfterPruning;
    }
}
