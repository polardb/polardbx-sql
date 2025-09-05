package com.alibaba.polardbx.executor.vectorized.build;

import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.TddlRelDataTypeSystemImpl;
import com.alibaba.polardbx.optimizer.core.TddlTypeFactoryImpl;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.expression.calc.DynamicParamExpression;
import com.alibaba.polardbx.optimizer.core.rel.OSSTableScan;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.boundspec.PartitionBoundVal;
import com.alibaba.polardbx.optimizer.partition.boundspec.PartitionBoundValueKind;
import com.alibaba.polardbx.optimizer.partition.common.PartKeyLevel;
import com.alibaba.polardbx.optimizer.partition.common.PartitionStrategy;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionField;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionFieldBuilder;
import com.alibaba.polardbx.optimizer.partition.pruning.ComparisonKind;
import com.alibaba.polardbx.optimizer.partition.pruning.PartPrunedResult;
import com.alibaba.polardbx.optimizer.partition.pruning.PartRouteFunction;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPrunerUtils;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionRouter;
import com.alibaba.polardbx.optimizer.partition.pruning.PhysicalPartitionInfo;
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumInfo;
import com.alibaba.polardbx.optimizer.utils.ExprContextProvider;
import com.google.common.base.Preconditions;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class InExpressionParamRewriter {
    private final static RelDataTypeFactory TYPE_FACTORY =
        new TddlTypeFactoryImpl(TddlRelDataTypeSystemImpl.getInstance());

    // index=1, {phy table1, list=[4,7]}, {phy table2, list=[1,2,8]}
    // index=3, {phy table1, list=[3,9]}, {phy table2, list=[0,4,6]}
    public static Map<Integer, Map<String, List>> rewriterParams(OSSTableScan ossTableScan,
                                                                 ExecutionContext executionContext) {

        final String logicalSchemaName = ossTableScan.getSchemaName();
        final String logicalTableName = ossTableScan.getLogicalTableName();
        if (ossTableScan.getOrcNode().getFilters().isEmpty()) {
            // no filter.
            return null;
        }
        RexNode rexNode = ossTableScan.getOrcNode().getFilters().get(0);

        TableMeta tableMeta = executionContext.getSchemaManager(logicalSchemaName)
            .getTable(logicalTableName);

        PartitionInfo partitionInfo = tableMeta.getPartitionInfo();
        if (partitionInfo.getPartitionBy().getSubPartitionBy() != null) {
            // only support 1-level hash partition without sub-partition.
            return null;
        }

        if (partitionInfo.getPartitionBy().getFullPartitionColumnMetas().size() != 1) {
            // only support single column key.
            return null;
        }

        InExpressionVisitor visitor = new InExpressionVisitor(ossTableScan, executionContext);
        rexNode.accept(visitor);
        return visitor.results;
    }

    private static class InExpressionVisitor extends RexVisitorImpl<Void> {
        Map<Integer, Map<String, List>> results = new HashMap<>();

        OSSTableScan ossTableScan;
        ExecutionContext executionContext;
        ExprContextProvider contextProvider;

        String logicalSchemaName;
        String logicalTableName;

        TableMeta tableMeta;
        PartitionInfo partitionInfo;
        int partitionCount;
        ColumnMeta partitionKeyColumnMeta;
        int partitionKeyColumnIndex;
        DataType partitionKeyColumnType;

        protected InExpressionVisitor(OSSTableScan ossTableScan, ExecutionContext executionContext) {
            super(true);

            this.ossTableScan = ossTableScan;
            this.executionContext = executionContext;
            this.contextProvider = new ExprContextProvider(executionContext);
            this.logicalSchemaName = ossTableScan.getSchemaName();
            this.logicalTableName = ossTableScan.getLogicalTableName();
            this.tableMeta = executionContext.getSchemaManager(logicalSchemaName).getTable(logicalTableName);
            this.partitionInfo = tableMeta.getPartitionInfo();
            this.partitionCount = partitionInfo.getPartitionBy().getPartitions().size();

            // Only accept: 1-level partition and single partition key.
            Preconditions.checkArgument(partitionInfo.getPartitionBy().getSubPartitionBy() == null);
            Preconditions.checkArgument(partitionInfo.getPartitionBy().getFullPartitionColumnMetas().size() == 1);
            this.partitionKeyColumnMeta = partitionInfo.getPartitionBy().getFullPartitionColumnMetas().get(0);
            this.partitionKeyColumnType = partitionKeyColumnMeta.getDataType();

            List<ColumnMeta> columnMetaList = tableMeta.getAllColumns();
            this.partitionKeyColumnIndex = columnMetaList.indexOf(partitionKeyColumnMeta);
        }

        @Override
        public Void visitCall(RexCall call) {
            if (call.getOperator() == TddlOperatorTable.IN || call.getOperator() == TddlOperatorTable.NOT_IN) {

                RexNode rexNode1 = call.getOperands().get(0);
                RexNode rexNode2 = call.getOperands().get(1);
                if (!(rexNode1 instanceof RexInputRef) || !(rexNode2 instanceof RexCall)) {
                    // do nothing.
                    return null;
                }
                RexInputRef inputRef = (RexInputRef) rexNode1;
                RexCall rowExpression = (RexCall) rexNode2;

                // Check if inputRef is partition key
                List<Integer> inProjects = ossTableScan.getOrcNode().getInProjects();
                Integer columnIndexOfTable = inProjects.get(inputRef.getIndex());
                if (columnIndexOfTable == null || columnIndexOfTable.intValue() != partitionKeyColumnIndex) {
                    return null;
                }

                // row expression expand
                for (RexNode operand : rowExpression.getOperands()) {

                    if (operand instanceof RexDynamicParam) {
                        RexDynamicParam dynamicParam = (RexDynamicParam) operand;

                        // evaluate dynamic param
                        final int dynamicParamIndex = dynamicParam.getIndex();
                        Object value = extractDynamicValue(dynamicParam);

                        // check if list value
                        if (value instanceof List) {
                            // row(RawString)

                            for (Object listItem : (List) value) {
                                if (listItem instanceof List) {
                                    // cannot expand: row in format of ((1,2,3), (2,3,4))
                                    return null;
                                }

                                // every list item is expanded from raw-string.
                                if (!(listItem instanceof Integer) && !(listItem instanceof Long)) {
                                    // unknown constant value.
                                    results.remove(dynamicParamIndex);
                                    return null;
                                }
                                List<String> phyTables = calcPartition(listItem);

                                // handle physical table, dynamic index, list item value
                                results.compute(dynamicParamIndex, (index, m) -> {
                                    if (m == null) {
                                        m = new HashMap<>();
                                    }

                                    // for 1 dynamic index and multi physical tables
                                    for (String phyTable : phyTables) {
                                        m.compute(phyTable, (p, list) -> {
                                            if (list == null) {
                                                list = new ArrayList<>();
                                            }

                                            list.add(listItem);
                                            return list;
                                        });
                                    }

                                    return m;
                                });

                            }
                        } else if (value instanceof String || value instanceof Number) {
                            // row(list: o1, o2 ...)

                            // prepare mode
                            // every list item is expanded from raw-string.
                            if (!(value instanceof Integer) && !(value instanceof Long)) {
                                // unknown constant value.
                                results.remove(dynamicParamIndex);
                                return null;
                            }
                            List<String> phyTables = calcPartition(value);

                            // handle physical table, dynamic index, list item value
                            results.compute(dynamicParamIndex, (index, m) -> {
                                if (m == null) {
                                    m = new HashMap<>();
                                }

                                // for 1 dynamic index and multi physical tables
                                for (String phyTable : phyTables) {
                                    m.compute(phyTable, (p, list) -> {
                                        if (list == null) {
                                            list = new ArrayList<>();
                                        }

                                        list.add(value);
                                        return list;
                                    });
                                }

                                return m;
                            });

                        } else {
                            // unknown mode.
                            results.remove(dynamicParamIndex);
                            return null;
                        }
                    } else {
                        // must be dynamic params for pruning.
                        return null;
                    }
                }

            }

            return super.visitCall(call);
        }

        private List<String> calcPartition(Object listItem) {
            SqlTypeName typeName = DataTypeUtil.typeNameOfParam(listItem);
            RelDataType relDataType = TYPE_FACTORY.createSqlType(typeName);
            DataType dataType = DataTypeUtil.calciteToDrdsType(relDataType);

            PartitionField partitionField = PartitionFieldBuilder.createField(partitionKeyColumnType);
            partitionField.store(listItem, dataType);

            PartitionRouter router = PartRouteFunction
                .getRouterByPartInfo(PartKeyLevel.PARTITION_KEY, null, partitionInfo);

            PartitionBoundVal partitionBoundVal = PartitionBoundVal.createPartitionBoundVal(
                partitionField, PartitionBoundValueKind.DATUM_NORMAL_VALUE
            );

            SearchDatumInfo searchDatumInfo = new SearchDatumInfo(partitionBoundVal);

            PartitionRouter.RouterResult result = router.routePartitions(
                executionContext, ComparisonKind.EQUAL, searchDatumInfo);

            BitSet allPartBitSet = PartitionPrunerUtils.buildEmptyPartitionsBitSetByPartRouter(router);
            if (result.getStrategy() != PartitionStrategy.LIST
                && result.getStrategy() != PartitionStrategy.LIST_COLUMNS) {
                PartitionPrunerUtils
                    .setPartBitSetByStartEnd(allPartBitSet, result.getPartStartPosi(), result.getPasrEndPosi(), true);
            } else {
                PartitionPrunerUtils
                    .setPartBitSetForPartList(allPartBitSet, result.getPartPosiSet(), true);
            }

            PartPrunedResult partPrunedResult = PartPrunedResult.buildPartPrunedResult(
                partitionInfo, allPartBitSet, PartKeyLevel.PARTITION_KEY, null, false);

            List<PhysicalPartitionInfo> physicalPartitionInfos = partPrunedResult.getPrunedPartitions();
            return physicalPartitionInfos.stream().map(PhysicalPartitionInfo::getPhyTable).collect(Collectors.toList());
        }

        private Object extractDynamicValue(RexDynamicParam dynamicParam) {
            // pre-compute the dynamic value when binging expression.
            DynamicParamExpression dynamicParamExpression =
                DynamicParamExpression.create(dynamicParam.getIndex(), contextProvider,
                    dynamicParam.getSubIndex(), dynamicParam.getSkIndex());

            return dynamicParamExpression.eval(null, executionContext);
        }

        @Override
        public Void visitDynamicParam(RexDynamicParam dynamicParam) {
            return super.visitDynamicParam(dynamicParam);
        }

        @Override
        public Void visitInputRef(RexInputRef inputRef) {
            return super.visitInputRef(inputRef);
        }
    }
}
