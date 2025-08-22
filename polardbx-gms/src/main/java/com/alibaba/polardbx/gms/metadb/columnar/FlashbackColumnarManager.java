package com.alibaba.polardbx.gms.metadb.columnar;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.oss.ColumnarFileType;
import com.alibaba.polardbx.common.oss.ColumnarPartitionPrunedSnapshot;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.gms.metadb.table.ColumnarAppendedFilesAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarAppendedFilesRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarCheckpointsAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableMappingAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableMappingRecord;
import com.alibaba.polardbx.gms.metadb.table.FilesAccessor;
import com.alibaba.polardbx.gms.metadb.table.FilesRecordSimplified;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;

public class FlashbackColumnarManager {

    private final Map<String, List<Pair<String, Long>>> deletePositionMap = new HashMap<>();
    private final Map<String, Pair<List<String>, List<Pair<String, Long>>>> snapshotInfo = new HashMap<>();
    private final Map<String, Long> schemaTsoMap = new HashMap<>();
    private Long columnarDeltaCheckpointTso = null;

    public FlashbackColumnarManager(long flashbackTso, String logicalSchema, String logicalTable,
                                    boolean autoPosition) {
        try (Connection connection = MetaDbUtil.getConnection()) {
            ColumnarTableMappingAccessor accessor = new ColumnarTableMappingAccessor();
            accessor.setConnection(connection);

            List<ColumnarTableMappingRecord> records = accessor.querySchemaIndex(logicalSchema, logicalTable);

            Long tableId;
            if (records != null && !records.isEmpty()) {
                ColumnarTableMappingRecord record = records.get(0);
                tableId = record.tableId;
            } else {
                throw new TddlRuntimeException(ErrorCode.ERR_COLUMNAR_SNAPSHOT,
                    String.format("Columnar index not found, schema: %s, table: %s", logicalSchema, logicalTable));
            }
            FilesAccessor filesAccessor = new FilesAccessor();
            filesAccessor.setConnection(connection);

            List<FilesRecordSimplified> snapshotFiles = filesAccessor
                .queryColumnarSnapshotFilesByTsoAndTableId(flashbackTso, logicalSchema, String.valueOf(tableId));

            if (autoPosition) {
                ColumnarCheckpointsAccessor checkpointsAccessor = new ColumnarCheckpointsAccessor();
                checkpointsAccessor.setConnection(connection);

                this.columnarDeltaCheckpointTso =
                    checkpointsAccessor.queryColumnarTsoByBinlogTsoAndCheckpointTsoAsc(flashbackTso)
                        .get(0).checkpointTso;
            }

            for (FilesRecordSimplified record : snapshotFiles) {
                String fileName = record.fileName;
                String partName = record.partitionName;
                Long schemaTso = record.schemaTs;
                String suffix = fileName.substring(fileName.lastIndexOf('.') + 1);
                ColumnarFileType columnarFileType = ColumnarFileType.of(suffix);

                if (columnarFileType == ColumnarFileType.ORC) {
                    snapshotInfo.computeIfAbsent(
                        partName,
                        s -> Pair.of(new ArrayList<>(), new ArrayList<>())
                    ).getKey().add(fileName);
                    schemaTsoMap.put(fileName, schemaTso);
                } else if (columnarFileType.isDeltaFile()) {
                    schemaTsoMap.put(fileName, schemaTso);

                    if (autoPosition) {
                        if (columnarFileType == ColumnarFileType.CSV) {
                            snapshotInfo.computeIfAbsent(
                                partName,
                                s -> Pair.of(new ArrayList<>(), new ArrayList<>())
                            ).getValue().add(Pair.of(fileName, -1L));
                        } else if (columnarFileType == ColumnarFileType.DEL) {
                            deletePositionMap.computeIfAbsent(
                                partName,
                                s -> new ArrayList<>()
                            ).add(Pair.of(fileName, -1L));
                        }
                    }
                }
            }

            if (autoPosition) {
                // for auto position, we get the read position from the csv file content, rather than GMS
                return;
            }

            ColumnarAppendedFilesAccessor appendedFilesAccessor = new ColumnarAppendedFilesAccessor();
            appendedFilesAccessor.setConnection(connection);

            List<ColumnarAppendedFilesRecord> appendedFilesRecords =
                appendedFilesAccessor.queryLastValidAppendByTsoAndTableId(flashbackTso, logicalSchema,
                    String.valueOf(tableId));

            for (ColumnarAppendedFilesRecord record : appendedFilesRecords) {
                String fileName = record.fileName;
                String partName = record.partName;
                long position = record.appendOffset + record.appendLength;
                String suffix = fileName.substring(fileName.lastIndexOf('.') + 1);
                ColumnarFileType columnarFileType = ColumnarFileType.of(suffix);

                if (columnarFileType == ColumnarFileType.CSV) {
                    snapshotInfo.computeIfAbsent(
                        partName,
                        s -> Pair.of(new ArrayList<>(), new ArrayList<>())
                    ).getValue().add(Pair.of(fileName, position));
                }

                if (columnarFileType == ColumnarFileType.DEL) {
                    deletePositionMap.computeIfAbsent(
                        partName,
                        s -> new ArrayList<>()
                    ).add(Pair.of(fileName, position));
                }
            }
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_COLUMNAR_SNAPSHOT,
                String.format("Failed to generate columnar snapshot, tso: %d, schema: %s, table: %s",
                    flashbackTso, logicalSchema, logicalTable));
        }
    }

    /**
     * @return partName -> ([orcFiles](name), [csvFiles](name, pos))
     */
    public Map<String, Pair<List<String>, List<Pair<String, Long>>>> getSnapshotInfo() {
        return snapshotInfo;
    }

    public Map<String, List<Pair<String, Long>>> getDeletePositions() {
        return deletePositionMap;
    }

    public Long getColumnarDeltaCheckpointTso() {
        return columnarDeltaCheckpointTso;
    }

    public Map<String, ColumnarPartitionPrunedSnapshot> getSnapshotInfo(
        SortedMap<Long, Set<String>> partitionResult) {
        Map<String, ColumnarPartitionPrunedSnapshot> result = new HashMap<>();
        snapshotInfo.forEach((partName, partSnapshot) -> {
            for (String orcFileName : partSnapshot.getKey()) {
                Long schemaTso = schemaTsoMap.get(orcFileName);
                SortedMap<Long, Set<String>> headMap = partitionResult.headMap(schemaTso + 1);
                if (!headMap.isEmpty()) {
                    Set<String> partitionSet = headMap.get(headMap.lastKey());
                    if (partitionSet != null && partitionSet.contains(partName)) {
                        result.computeIfAbsent(partName,
                            s -> new ColumnarPartitionPrunedSnapshot()
                        ).getOrcFilesAndSchemaTs().add(Pair.of(orcFileName, schemaTso));
                    }
                }
            }

            for (Pair<String, Long> csvNameAndPos : partSnapshot.getValue()) {
                Long schemaTso = schemaTsoMap.get(csvNameAndPos.getKey());
                SortedMap<Long, Set<String>> headMap = partitionResult.headMap(schemaTso + 1);
                if (!headMap.isEmpty()) {
                    Set<String> partitionSet = headMap.get(headMap.lastKey());
                    if (partitionSet != null && partitionSet.contains(partName)) {
                        result.computeIfAbsent(partName,
                                s -> new ColumnarPartitionPrunedSnapshot()
                            ).getCsvFilesAndSchemaTsWithPos()
                            .add(Pair.of(csvNameAndPos.getKey(), Pair.of(schemaTso, csvNameAndPos.getValue())));
                    }
                }
            }
        });
        return result;
    }

    public Map<String, List<Pair<String, Long>>> getDeletePositions(
        SortedMap<Long, Set<String>> partitionResult) {
        Map<String, List<Pair<String, Long>>> result = new HashMap<>();
        deletePositionMap.forEach((partName, partDeletePositions) -> {
            for (Pair<String, Long> deletePosition : partDeletePositions) {
                Long schemaTso = schemaTsoMap.get(deletePosition.getKey());
                SortedMap<Long, Set<String>> headMap = partitionResult.headMap(schemaTso + 1);
                if (!headMap.isEmpty()) {
                    Set<String> partitionSet = headMap.get(headMap.lastKey());
                    if (partitionSet != null && partitionSet.contains(partName)) {
                        result.computeIfAbsent(partName, s -> new ArrayList<>()).add(deletePosition);
                    }
                }
            }
        });
        return result;
    }
}
