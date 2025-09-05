package com.alibaba.polardbx.optimizer.utils;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.util.TargetTableInfo;
import com.alibaba.polardbx.optimizer.core.rel.util.TargetTableInfoOneTable;
import com.alibaba.polardbx.optimizer.partition.PartSpecBase;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.partition.pruning.PartPrunedResult;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPrunerUtils;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.util.ImmutableIntList;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_EXECUTOR;

public class PartitionUtils {
    /**
     * Determines whether the given logical view corresponds to a new partitioned table.
     *
     * @param logicalView The logical view object used to retrieve the logical table name and underlying physical table information.
     * @return boolean Returns true if the table is both a new partitioned table and has been sharded; otherwise, returns false.
     */
    public static boolean isNewPartShardTable(LogicalView logicalView) {
        String tableName = logicalView.getLogicalTableName();
        TableMeta tm = CBOUtil.getTableMeta(logicalView.getTable());
        TddlRuleManager manger = OptimizerContext.getContext(tm.getSchemaName()).getRuleManager();
        if (!manger.getPartitionInfoManager().isNewPartDbTable(tableName) || !manger.isShard(tableName)) {
            return false;
        }
        return true;
    }

    /**
     * Checks whether the partitions of a table are ordered as required.
     *
     * @param tableInfo An object containing partition usage and sorting status information for the table.
     * @return boolean Returns true if the table's partitions are orderly; otherwise, returns false.
     */
    public static boolean isTablePartOrdered(TargetTableInfoOneTable tableInfo) {
        boolean useSubPart = tableInfo.isUseSubPart();
        if (!useSubPart) {
            // If sub-partitions are not used, check if all partitions are sorted
            if (tableInfo.isAllPartSorted()) {
                return true;
            }
        } else {
            // NOTE: The 'sorted' mentioned here refers to the order of the level-one partition key.
            // sub-partitions are used
            // Check if every partition has only one subpartition after pruning
            // Check if all partitions are sorted
            if (tableInfo.isAllPrunedPartContainOnlyOneSubPart()
                && tableInfo.isAllPartSorted()) {
                return true;
            }
            // Check if the count of first-level partitions is 1
            // Check if all sub-partitions are sorted
            if (tableInfo.getPrunedFirstLevelPartCount() == 1
                && tableInfo.isAllSubPartSorted()) {
                return true;
            }
        }
        return false;
    }

    public static boolean isOrderKeyMatched(LogicalView logicalView, TargetTableInfoOneTable tableInfo) {
        List<String> partitionColumns =
            tableInfo.isUseSubPart() ? tableInfo.getSubpartColList() : tableInfo.getPartColList();
        return isOrderKeyMatched(logicalView, partitionColumns);
    }

    /**
     * Checks whether the ordering keys in the logical view match the given partition columns.
     *
     * @param logicalView The LogicalView object containing the structure and optimized query information for the table to be checked.
     * @param partitionColumns A list of partition column names to be verified against the ordering keys.
     * @return Returns true if the ordering keys match the partition columns; otherwise, returns false.
     */
    public static boolean isOrderKeyMatched(LogicalView logicalView, List<String> partitionColumns) {
        List<Integer> partColumnRefs = new ArrayList<>();
        for (String partitionColumn : partitionColumns) {
            partColumnRefs.add(
                logicalView.getRefByColumnName(logicalView.getTableNames().get(0), partitionColumn, false, false));
        }
        // part column not found in the logical view
        if (partColumnRefs.stream().anyMatch(ref -> ref < 0)) {
            return false;
        }
        RelNode pushedRelNode = logicalView.getOptimizedPushedRelNodeForMetaQuery();
        // pushed relNode is not sort, just return false
        if (!(pushedRelNode instanceof Sort)) {
            return false;
        }
        ImmutableIntList orderKeys = ((Sort) pushedRelNode).collation.getKeys();
        // The partition key should be a prefix of the sort key
        if (partitionColumns.size() > orderKeys.size()) {
            return false;
        }
        for (int i = 0; i < partColumnRefs.size(); ++i) {
            if (!partColumnRefs.get(i).equals(orderKeys.get(i))) {
                return false;
            }
        }
        // ordered direction should be equal, otherwise split cannot be order
        List<RelFieldCollation> collations =
            ((Sort) pushedRelNode).collation.getFieldCollations().subList(0, partColumnRefs.size());
        long directionCount = collations.stream().map(col -> col.direction).distinct().count();
        if (directionCount > 1) {
            return false;
        }
        return true;
    }

    /**
     * Calculates the partition number of the given logical table under the specified physical schema and physical table name.
     *
     * @param logicalSchema Name of the logical schema.
     * @param logicalTableName Name of the logical table.
     * @param physicalSchema Name of the physical schema.
     * @param physicalTableName Name of the physical table.
     * @return Partition number. Returns -1 if the corresponding partition is not found.
     */
    public static int calcPartition(String logicalSchema, String logicalTableName, String physicalSchema,
                                    String physicalTableName, ExecutionContext ec) {
        // Retrieves the partition information for the specified logical table
        PartitionInfo partInfo;
        if (isInvalidPartitionInfo(ec, logicalSchema, logicalTableName)) {
            partInfo = getPartitionInfoFromOptimizerContext(logicalSchema, logicalTableName);
        } else {
            partInfo = getPartitionInfoFromEC(ec, logicalSchema, logicalTableName);
        }

        // Obtains the list of partition specifications
        List<PartitionSpec> partitionSpecs = partInfo.getPartitionBy().getPhysicalPartitions();

        // Streams through partition specifications to find a match where the physical schema and physical table name equal the input parameters
        int partition = partitionSpecs.stream().filter(
                t -> t.getLocation().getGroupKey().equalsIgnoreCase(physicalSchema)
                    && t.getLocation().getPhyTableName().equalsIgnoreCase(physicalTableName))
            .findFirst().map(PartSpecBase::getPosition).map(Long::intValue).orElse(-1);

        // Adjusts and returns the found partition number, subtracting 1 to start counting from 0
        return partition - 1;
    }

    private static boolean isInvalidPartitionInfo(ExecutionContext ec, String logicalSchema, String logicalTableName) {
        return ec == null ||
            ec.getSchemaManager(logicalSchema) == null ||
            ec.getSchemaManager(logicalSchema).getTable(logicalTableName) == null ||
            ec.getSchemaManager(logicalSchema).getTable(logicalTableName).getPartitionInfo() == null;
    }

    private static PartitionInfo getPartitionInfoFromOptimizerContext(String logicalSchema, String logicalTableName) {
        return OptimizerContext.getContext(logicalSchema).getPartitionInfoManager().getPartitionInfo(logicalTableName);
    }

    private static PartitionInfo getPartitionInfoFromEC(ExecutionContext ec, String logicalSchema,
                                                        String logicalTableName) {
        return ec.getSchemaManager(logicalSchema).getTable(logicalTableName).getPartitionInfo();
    }

    /**
     * @return <partition number, <physical schema, physical table name>>
     */
    public static Pair<Integer, Pair<String, String>> calcPartition(PartitionInfo partInfo, String partName) {

        Optional<PartitionSpec> partitionSpec =
            partInfo.getPartitionBy().getPartitions().stream()
                .filter(part -> part.getName().equalsIgnoreCase(partName)).findFirst();
        int partition = partitionSpec.map(PartSpecBase::getPosition).map(Long::intValue).orElse(-1);
        String phySchema = partitionSpec.map(part -> part.getLocation().getGroupKey()).orElse(null);
        String phyTableName = partitionSpec.map(part -> part.getLocation().getPhyTableName()).orElse(null);
        return Pair.of(partition - 1, Pair.of(phySchema, phyTableName));
    }

    public static boolean checkPrunedPartitionMonotonic(@NotNull LogicalView logicalView,
                                                        @NotNull List<PartPrunedResult> partitionResult) {
        final TargetTableInfo targetTableInfo =
            PartitionPrunerUtils.buildTargetTableInfoByPartPrunedResults(partitionResult);

        final TargetTableInfoOneTable tableInfo = targetTableInfo.getTargetTableInfoList().get(0);
        if (!isTablePartOrdered(tableInfo)) {
            return false;
        }
        if (!isOrderKeyMatched(logicalView, tableInfo)) {
            return false;
        }

        return true;
    }

    @NotNull
    public static List<Integer> sortByPartitionOrder(String logicalSchemaName, String logicalTableName,
                                                     List<Pair<String, String>> phySchemaAndPhyTables, boolean isDesc) {
        final PartitionInfo partInfo = OptimizerContext
            .getContext(logicalSchemaName)
            .getPartitionInfoManager()
            .getPartitionInfo(logicalTableName);

        //noinspection unchecked
        final List<Integer> partitions = Ord.zip(phySchemaAndPhyTables)
            .stream()
            .map(ord -> Pair.of(
                partInfo.getPartSpecSearcher().getPartSpec(ord.e.getKey(), ord.e.getValue()).getPhyPartPosition(),
                ord.i))
            .sorted((p1, p2) -> (int) (!isDesc ? p1.getKey() - p2.getKey() : p2.getKey() - p1.getKey()))
            .map(Pair::getValue)
            .collect(Collectors.toList());
        boolean notFoundPart = partitions.stream().anyMatch(part -> part < 0);
        if (notFoundPart) {
            throw new TddlRuntimeException(ERR_EXECUTOR, "not found partition info");
        }
        return partitions;
    }
}
