package com.alibaba.polardbx.executor.columnar.pruning;

import com.alibaba.polardbx.executor.columnar.pruning.index.IndexPruner;
import com.alibaba.polardbx.executor.operator.scan.impl.PreheatFileMeta;
import com.alibaba.polardbx.gms.module.LogLevel;
import com.alibaba.polardbx.gms.module.LogPattern;
import com.alibaba.polardbx.gms.module.Module;
import com.alibaba.polardbx.gms.module.ModuleLogInfo;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheStats;
import org.apache.hadoop.fs.Path;
import org.apache.orc.OrcProto;
import org.apache.orc.impl.OrcIndex;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.alibaba.polardbx.gms.engine.FileStoreStatistics.CACHE_STATS_FIELD_COUNT;

/**
 * @author fangwu
 */
public class ColumnarPruneManager {

    private static final String PRUNER_CACHE_NAME = "PRUNER_CACHE";

    private static final int PRUNER_CACHE_MAX_ENTRY = 1024 * 256;

    private static final int PRUNER_CACHE_TTL_HOURS = 12;

    private static final Cache<Path, IndexPruner> PRUNER_CACHE =
        CacheBuilder.newBuilder()
            .recordStats()
            .maximumSize(PRUNER_CACHE_MAX_ENTRY)
            .expireAfterAccess(PRUNER_CACHE_TTL_HOURS, TimeUnit.HOURS)
            .softValues()
            .build();

    public static IndexPruner getIndexPruner(Path targetFile,
                                             PreheatFileMeta preheat,
                                             List<ColumnMeta> columns, int clusteringKeyPosition,
                                             List<Integer> orcIndexes,
                                             boolean enableOssCompatible)
        throws ExecutionException {
        return PRUNER_CACHE.get(targetFile, () -> {
            IndexPruner.IndexPrunerBuilder builder =
                new IndexPruner.IndexPrunerBuilder(targetFile.toString(), enableOssCompatible);
            int rgNum = 0;
            for (int i = 0; i < preheat.getPreheatStripes().size(); i++) {
                OrcIndex stripeIndex = preheat.getOrcIndex(i);
                // init sort key index if exist
                if (clusteringKeyPosition != -1) {
                    // alter sort key column is not supported
                    Preconditions.checkArgument(orcIndexes.get(clusteringKeyPosition) != null);
                    OrcProto.RowIndex rgIndex = stripeIndex.getRowGroupIndex()[orcIndexes.get(clusteringKeyPosition)];
                    builder.setSortKeyColId(clusteringKeyPosition);
                    builder.setSortKeyDataType(columns.get(clusteringKeyPosition).getDataType());
                    // init sort key index
                    for (OrcProto.RowIndexEntry rowIndexEntry : rgIndex.getEntryList()) {
                        rgNum++;
                        builder.appendSortKeyIndex(rowIndexEntry.getStatistics().getIntStatistics());
                    }
                } else {
                    // error log
                    ModuleLogInfo.getInstance().logRecord(
                        Module.COLUMNAR_PRUNE,
                        LogPattern.UNEXPECTED,
                        new String[] {"sort key load", "neg clustering key position " + clusteringKeyPosition},
                        LogLevel.CRITICAL);
                }

                for (int m = 0; m < columns.size(); m++) {
                    // skip sort key index
                    if (m == clusteringKeyPosition) {
                        continue;
                    }
                    ColumnMeta cm = columns.get(m);
                    // get orc column index by table meta column index
                    Integer orcIndex = orcIndexes.get(m);
                    if (orcIndex == null) {
                        continue;
                    }

                    DataType dataType = cm.getDataType();// zone map
                    if (DataTypeUtil.equalsSemantically(DataTypes.IntegerType, dataType) ||
                        DataTypeUtil.equalsSemantically(DataTypes.LongType, dataType) ||
                        DataTypeUtil.equalsSemantically(DataTypes.DateType, dataType) ||
                        DataTypeUtil.equalsSemantically(DataTypes.DatetimeType, dataType)) {
                        builder.appendZoneMap(m, dataType);
                        OrcProto.RowIndex rgIndex = stripeIndex.getRowGroupIndex()[orcIndex];
                        for (OrcProto.RowIndexEntry rowIndexEntry : rgIndex.getEntryList()) {
                            OrcProto.ColumnStatistics columnStatistics = rowIndexEntry.getStatistics();
                            // zone map index build
                            if (columnStatistics.hasIntStatistics()) {
                                builder.appendZoneMap(m, rowIndexEntry.getStatistics().getIntStatistics());
                            } else if (columnStatistics.hasDateStatistics()) {
                                builder.appendZoneMap(m, rowIndexEntry.getStatistics().getDateStatistics());
                            }
                            if (columnStatistics.hasHasNull()) {
                                builder.appendZoneMap(m, rowIndexEntry.getStatistics().getHasNull());
                            }
                        }
                    } else if (DataTypes.DecimalType.equals(dataType)) {
                        // TODO build zone map index for DecimalType
                    } else {
                        continue;
                    }
                }

                // bitmap index was built by stripe index

                // TODO bloom filter build
                builder.stripeEnd();
            }

            builder.setRgNum(rgNum);
            return builder.build();
        });
    }

    private static int getMetaIndex(List<Integer> orcIndexes, int orcIndex) {
        for (int i = 0; i < orcIndexes.size(); i++) {
            if (orcIndexes.get(i) == orcIndex) {
                return i;
            }
        }
        throw new IllegalArgumentException("orc column index not match:" + orcIndex + "," + orcIndexes);
    }

    public static byte[][] getCacheStat() {
        CacheStats cacheStats = PRUNER_CACHE.stats();

        byte[][] results = new byte[CACHE_STATS_FIELD_COUNT][];
        int pos = 0;
        results[pos++] = PRUNER_CACHE_NAME.getBytes();
        results[pos++] = String.valueOf(-1).getBytes();
        results[pos++] = String.valueOf(PRUNER_CACHE.size()).getBytes();
        results[pos++] = String.valueOf(-1).getBytes();
        results[pos++] = String.valueOf(cacheStats.hitCount()).getBytes();
        results[pos++] = String.valueOf(-1).getBytes();
        results[pos++] = String.valueOf(cacheStats.missCount()).getBytes();
        results[pos++] = String.valueOf(-1).getBytes();
        results[pos++] = String.valueOf(-1).getBytes();
        results[pos++] = "IN MEMORY".getBytes();
        results[pos++] = new StringBuilder().append(PRUNER_CACHE_TTL_HOURS).append(" h").toString().getBytes();
        results[pos++] = String.valueOf(PRUNER_CACHE_MAX_ENTRY).getBytes();
        results[pos++] = new StringBuilder().append(-1).append(" BYTES").toString().getBytes();
        return results;
    }
}
