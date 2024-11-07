package com.alibaba.polardbx.optimizer.core.rel.ddl;

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.archive.CheckOSSArchiveUtil;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableArchivePartitionPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartSpecSearcher;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.partition.common.PartKeyLevel;
import com.alibaba.polardbx.optimizer.ttl.TtlDefinitionInfo;
import org.apache.calcite.rel.ddl.AlterTableArchivePartition;
import org.apache.calcite.sql.SqlAlterTableArchivePartition;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

/**
 * @author wumu
 */
public class LogicalAlterTableArchivePartition extends LogicalTableOperation {
    private SqlAlterTableArchivePartition sqlAlterTableArchivePartition;

    protected AlterTableArchivePartitionPreparedData preparedData;

    public LogicalAlterTableArchivePartition(AlterTableArchivePartition archivePartition) {
        super(archivePartition);
        this.sqlAlterTableArchivePartition = (SqlAlterTableArchivePartition) relDdl.sqlNode;
    }

    @Override
    public boolean isSupportedByFileStorage() {
        return true;
    }

    @Override
    public boolean isSupportedByBindFileStorage() {
        return true;
    }

    public static LogicalAlterTableArchivePartition create(AlterTableArchivePartition archivePartition) {
        return new LogicalAlterTableArchivePartition(archivePartition);
    }

    public void prepare() {
        SqlAlterTableArchivePartition alterTableArchivePartition =
            (SqlAlterTableArchivePartition) this.getNativeSqlNode();

        preparedData = new AlterTableArchivePartitionPreparedData();

        Optional<Pair<String, String>> primaryTable = CheckOSSArchiveUtil.getTTLSource(schemaName, tableName);
        TableMeta primaryTableMeta = null;

        if (primaryTable.isPresent()) {
            String primaryTableSchema = GeneralUtil.coalesce(primaryTable.get().getKey(), schemaName);
            String primaryTableName = primaryTable.get().getValue();

            primaryTableMeta =
                OptimizerContext.getContext(primaryTableSchema).getLatestSchemaManager().getTable(primaryTableName);
            if (primaryTableMeta == null) {
                throw new TddlNestableRuntimeException(String.format(
                    "The ttl table bound to the oss table %s.%s does not exist.", schemaName, tableName));
            }

            TtlDefinitionInfo ttlDefinitionInfo = primaryTableMeta.getTtlDefinitionInfo();
            if (ttlDefinitionInfo == null) {
                throw new TddlNestableRuntimeException(String.format(
                    "The ttl table bound to the oss table %s.%s does not exist.", schemaName, tableName));
            }

            final String archiveTmpTableName = ttlDefinitionInfo.getTmpTableName();
            final String archiveTmpTableSchema =
                GeneralUtil.coalesce(ttlDefinitionInfo.getTmpTableSchema(), schemaName);

            preparedData.setPrimaryTableSchema(primaryTableSchema);
            preparedData.setPrimaryTableName(primaryTableName);
            preparedData.setTmpTableSchema(archiveTmpTableSchema);
            preparedData.setTmpTableName(archiveTmpTableName);
        } else {
            throw new TddlNestableRuntimeException(String.format(
                "The ttl table bound to the oss table %s.%s does not exist.", schemaName, tableName));
        }

        boolean isSubpartition = alterTableArchivePartition.isSubPartitionsArchive();
        if (isSubpartition) {
            throw new TddlNestableRuntimeException(String.format(
                "Subpartition is not support on this operation"));
        }

        TtlDefinitionInfo ttlDefinitionInfo = primaryTableMeta.getTtlDefinitionInfo();
        String archiveTmpTableName = ttlDefinitionInfo.getTmpTableName();
        String archiveTmpTableSchema = ttlDefinitionInfo.getTmpTableSchema();
        TableMeta arcTmpTableMeta =
            OptimizerContext.getContext(archiveTmpTableSchema).getLatestSchemaManager().getTable(archiveTmpTableName);
        PartitionInfo arcTmpTablePartInfo = arcTmpTableMeta.getPartitionInfo();
        Set<String> partitionNames = alterTableArchivePartition.getTargetPartitions();
        PartSpecSearcher partSpecSearcher = arcTmpTablePartInfo.getPartSpecSearcher();
        for (String partName : partitionNames) {
            PartitionSpec spec = partSpecSearcher.getPartSpecByPartName(partName);
            if (spec.getPartLevel() == PartKeyLevel.SUBPARTITION_KEY) {
                throw new TddlNestableRuntimeException(String.format(
                    "the subpartition `%s` is not allowed on this operation", partName));
            }
        }

        List<String> realPartitionNames = new ArrayList<>();
        if (isSubpartition) {
            realPartitionNames.addAll(partitionNames);
        } else {
            realPartitionNames.addAll(getArchivePartNamesByFirstPartNames(partitionNames));
        }
        preparedData.setPhyPartitionNames(realPartitionNames);
        List<String> firstPartNames = new ArrayList<>();
        firstPartNames.addAll(partitionNames);
        preparedData.setFirstLevelPartitionNames(firstPartNames);

    }

    public AlterTableArchivePartitionPreparedData getPreparedData() {
        return preparedData;
    }

    public SqlAlterTableArchivePartition getSqlAlterTableArchivePartition() {
        return sqlAlterTableArchivePartition;
    }

    private List<String> getArchivePartNamesByFirstPartNames(Set<String> partNames) {
        final TableMeta ossTableMeta =
            OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(tableName);

        if (ossTableMeta == null) {
            throw new TddlNestableRuntimeException(String.format(
                "The oss table %s.%s does not exist.", schemaName, tableName));
        }

        PartitionInfo partitionInfo = ossTableMeta.getPartitionInfo();

        Set<String> rs = new TreeSet<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        for (String partName : partNames) {
            PartitionSpec ps = partitionInfo.getPartSpecSearcher().getPartSpecByPartName(partName);
            if (ps == null) {
                throw new TddlNestableRuntimeException(String.format(
                    "The archive partition %s does not exists", partName));
            }
            List<PartitionSpec> subParts = ps.getSubPartitions();
            if (subParts != null) {
                for (PartitionSpec subPart : subParts) {
                    rs.add(subPart.getName());
                }
            } else {
                rs.add(partName);
            }
        }
        return new ArrayList<>(rs);
    }
}
