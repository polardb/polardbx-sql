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

package com.alibaba.polardbx.executor.columnar.pruning.index;

import com.alibaba.polardbx.executor.columnar.pruning.data.PruneUtils;
import com.alibaba.polardbx.executor.columnar.pruning.index.builder.BitMapRowGroupIndexBuilder;
import com.alibaba.polardbx.executor.columnar.pruning.index.builder.SortKeyIndexBuilder;
import com.alibaba.polardbx.executor.columnar.pruning.index.builder.ZoneMapIndexBuilder;
import com.alibaba.polardbx.executor.columnar.pruning.predicate.ColumnPredicatePruningInf;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.statis.ColumnarTracer;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.airlift.slice.Slice;
import org.apache.orc.OrcProto;
import org.jetbrains.annotations.NotNull;
import org.openjdk.jol.info.ClassLayout;
import org.roaringbitmap.RoaringBitmap;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * defined the order of prune indexes
 *
 * @author fangwu
 */
public class IndexPruner {

    private static final long INSTANCE_SIZE = ClassLayout.parseClass(IndexPruner.class).instanceSize();

    // target file meta
    private String orcFile;

    // index for prune
    /**
     * Total number of row groups in this orc file.
     */
    private final long rgNum;
    private SortKeyIndex sortKeyIndex;
    private BitMapRowGroupIndex bitMapRowGroupIndex;
    private ZoneMapIndex zoneMapIndex;
    private BloomFilterIndex bloomFilterIndex;
    /**
     * The i-th strip contains stripeRgNum[i] row groups.
     */
    private List<Integer> stripeRgNum;
    private long estimatedSizeInBytes;

    private IndexPruner(long rgNum) {
        this.rgNum = rgNum;
    }

    public SortedMap<Integer, boolean[]> pruneToSortMap(String table,
                                                        List<ColumnMeta> columns,
                                                        @Nonnull ColumnPredicatePruningInf cpp,
                                                        IndexPruneContext ipc) {
        RoaringBitmap rr = prune(table, columns, cpp, ipc);
        SortedMap<Integer, boolean[]> sortedMap = new TreeMap<>();
        int curRgNum = 0;
        for (int i = 0; i < stripeRgNum.size(); i++) {
            int curStripeRgNum = stripeRgNum.get(i);
            boolean[] bs = new boolean[curStripeRgNum];

            for (int j = 0; j < curStripeRgNum; j++) {
                if (rr.contains(j + curRgNum)) {
                    bs[j] = true;
                }
            }
            curRgNum = curRgNum + curStripeRgNum;
            sortedMap.put(i, bs);
        }
        return sortedMap;
    }

    public SortedMap<Integer, boolean[]> pruneToSortMap(@Nonnull RoaringBitmap rr) {
        SortedMap<Integer, boolean[]> sortedMap = new TreeMap<>();
        int curRgNum = 0;
        for (int i = 0; i < stripeRgNum.size(); i++) {
            int curStripeRgNum = stripeRgNum.get(i);
            boolean[] bs = new boolean[curStripeRgNum];

            for (int j = 0; j < curStripeRgNum; j++) {
                if (rr.contains(j + curRgNum)) {
                    bs[j] = true;
                }
            }
            curRgNum = curRgNum + curStripeRgNum;
            sortedMap.put(i, bs);
        }
        return sortedMap;
    }

    public RoaringBitmap prune(String table,
                               List<ColumnMeta> columns,
                               @Nonnull ColumnPredicatePruningInf cpp,
                               IndexPruneContext ipc) {
        RoaringBitmap pruneResult = RoaringBitmap.bitmapOfRange(0, rgNum);
        pruneIndex(table, cpp, ipc, PruneAction.SORT_KEY_INDEX_PRUNE, sortKeyIndex, pruneResult, columns);
//        pruneIndex(table, cpp, ipc, PruneAction.BITMAP_INDEX_PRUNE, bitMapRowGroupIndex, pruneResult, columns);
        pruneIndex(table, cpp, ipc, PruneAction.ZONE_MAP_INDEX_PRUNE, zoneMapIndex, pruneResult, columns);
        // TODO bloom filter pruning
        return pruneResult;
    }

    public RoaringBitmap pruneOnlyDeletedRowGroups(RoaringBitmap deleteBitmap) {

        return new RoaringBitmap();
    }

    private void pruneIndex(String table,
                            @NotNull ColumnPredicatePruningInf cpp,
                            IndexPruneContext ipc,
                            PruneAction pruneAction,
                            ColumnIndex columnIndex,
                            RoaringBitmap cur,
                            List<ColumnMeta> columns) {
        if (columnIndex == null) {
            return;
        }
        if (cur.isEmpty()) {
            return;
        }
        ColumnarTracer columnarTracer = ipc.getPruneTracer();
        int before = cur.getCardinality();
        pruneAction.getPruneAction().apply(cpp, columnIndex, ipc, cur);
        int after = cur.getCardinality();
        if (columnarTracer != null) {
            switch (pruneAction) {
            case SORT_KEY_INDEX_PRUNE:
                columnarTracer.tracePruneIndex(table, PruneUtils.display(cpp, columns, ipc), before - after, 0, 0);
                break;
            case ZONE_MAP_INDEX_PRUNE:
                columnarTracer.tracePruneIndex(table, PruneUtils.display(cpp, columns, ipc), 0, before - after, 0);
                break;
            case BITMAP_INDEX_PRUNE:
                columnarTracer.tracePruneIndex(table, PruneUtils.display(cpp, columns, ipc), 0, 0, before - after);
                break;
            }
        }
    }

    public String getOrcFile() {
        return orcFile;
    }

    public void setOrcFile(String orcFile) {
        this.orcFile = orcFile;
    }

    public long getRgNum() {
        return rgNum;
    }

    public List<Integer> getStripeRgNum() {
        return stripeRgNum;
    }

    public static class IndexPrunerBuilder {
        private long rgNum;
        private final String filePath;
        private final SortKeyIndexBuilder sortKeyIndexBuilder = new SortKeyIndexBuilder();
        private final ZoneMapIndexBuilder zoneMapIndexBuilder = new ZoneMapIndexBuilder();
        private final BitMapRowGroupIndexBuilder bitMapRowGroupIndexBuilder = new BitMapRowGroupIndexBuilder();
        private final List<Integer> stripeRgNum = Lists.newArrayList();
        private int curRgNum;

        // TODO replaced by builder
        private BloomFilterIndex bloomFilterIndex;

        private final boolean enableOssCompatible;

        public IndexPrunerBuilder(String filePath,
                                  boolean enableOssCompatible) {
            this.filePath = filePath;
            this.enableOssCompatible = enableOssCompatible;
        }

        public IndexPruner build() {
            IndexPruner indexPruner = new IndexPruner(rgNum);
            indexPruner.setOrcFile(filePath);
            indexPruner.sortKeyIndex = sortKeyIndexBuilder.build();
            indexPruner.bitMapRowGroupIndex = bitMapRowGroupIndexBuilder.build();
            indexPruner.zoneMapIndex = zoneMapIndexBuilder.build();
            indexPruner.bloomFilterIndex = bloomFilterIndex;
            indexPruner.stripeRgNum = stripeRgNum;
            indexPruner.updateSizeInfo();
            return indexPruner;
        }

        public IndexPrunerBuilder appendSortKeyIndex(OrcProto.ColumnStatistics columnStatistics) {
            sortKeyIndexBuilder.appendDataEntry(columnStatistics);
            curRgNum++;
            return this;
        }

        //only for test
        public IndexPrunerBuilder appendMockSortKeyIndex(Object min, Object max, DataType dt) {
            sortKeyIndexBuilder.appendMockDataEntry(min, max, dt);
            curRgNum++;
            return this;
        }

        public void setSortKeyColId(int colId) {
            sortKeyIndexBuilder.setColId(colId);
        }

        public void setSortKeyDataType(DataType dt) {
            sortKeyIndexBuilder.setDt(dt);
        }

        public void stripeEnd() {
            stripeRgNum.add(curRgNum);
            curRgNum = 0;
        }

        public void setBloomFilterIndex(BloomFilterIndex bloomFilterIndex) {
            this.bloomFilterIndex = bloomFilterIndex;
        }

        public void appendZoneMap(int columnId, OrcProto.IntegerStatistics intStatistics) {
            Long min = intStatistics.getMinimum();
            Long max = intStatistics.getMaximum();
            Preconditions.checkArgument(
                columnId >= 0 &&
                    min != null && max != null &&
                    min >= Long.MIN_VALUE && min <= Long.MAX_VALUE &&
                    max >= Long.MIN_VALUE && max <= Long.MAX_VALUE,
                "bad data for zone map index:" + columnId + ", int type," + min + "," + max);
            zoneMapIndexBuilder.appendLongData(columnId, min).appendLongData(columnId, max);
        }

        public void appendZoneMap(int columnId, OrcProto.DateStatistics dateStatistics) {
            Integer min = dateStatistics.getMinimum();
            Integer max = dateStatistics.getMaximum();
            Preconditions.checkArgument(
                columnId >= 0 &&
                    min != null && max != null,
                "bad data for zone map index:" + columnId + ", int type," + min + "," + max);
            zoneMapIndexBuilder.appendIntegerData(columnId, min).appendIntegerData(columnId, max);
        }

        public void appendZoneMap(int columnId, Long min, Long max) {
            Preconditions.checkArgument(
                columnId > 0 &&
                    min != null && max != null,
                "bad data for zone map index:" + columnId + ", int type," + min + "," + max);
            zoneMapIndexBuilder.appendLongData(columnId, min).appendLongData(columnId, max);
        }

        public void appendZoneMap(int columnId, boolean hasNull) {
            Preconditions.checkArgument(
                columnId >= 0,
                "bad data for zone map index:" + columnId);
            zoneMapIndexBuilder.appendNull(columnId, hasNull);
        }

        public void appendZoneMap(int columnId, DataType dataType) {
            Preconditions.checkArgument(columnId >= 0 && dataType != null,
                "bad data for zone map index:" + columnId + "," + dataType);
            zoneMapIndexBuilder.appendColumn(columnId, dataType);
        }

        public void setRgNum(int rgNum) {
            this.rgNum = rgNum;
            bitMapRowGroupIndexBuilder.setRgNum(rgNum);
        }

        public void appendBitmap(int columnId, List<OrcProto.BitmapColumn> bitMapIndexList) throws IOException {
            // bit map not support this column, just return
            if (!bitMapRowGroupIndexBuilder.supportColumn(columnId)) {
                return;
            }
            for (OrcProto.BitmapColumn bitmapColumn : bitMapIndexList) {
                RoaringBitmap rb = new RoaringBitmap();
                rb.deserialize(ByteBuffer.wrap(bitmapColumn.getBitmap().toByteArray()));
                bitMapRowGroupIndexBuilder.appendValue(columnId, bitmapColumn.getVal().toStringUtf8(), rb);
            }
        }

        public void appendBitmap(int columnId, DataType dt) {
            Preconditions.checkArgument(columnId >= 0 && dt != null,
                "bad data for zone map index:" + columnId + "," + dt);
            // disable bitmap when oss compatible and column type is char/varchar
            if (!(enableOssCompatible &&
                (dt.getDataClass() == String.class || dt.getDataClass() == Slice.class))) {
                bitMapRowGroupIndexBuilder.appendColumn(columnId, dt);
            }
        }
    }

    public long getSizeInBytes() {
        return estimatedSizeInBytes;
    }

    private void updateSizeInfo() {
        long size = INSTANCE_SIZE;
        if (sortKeyIndex != null) {
            size += sortKeyIndex.getSizeInBytes();
        }
        if (bitMapRowGroupIndex != null) {
            size += bitMapRowGroupIndex.getSizeInBytes();
        }
        if (zoneMapIndex != null) {
            size += zoneMapIndex.getSizeInBytes();
        }
        if (bloomFilterIndex != null) {
            size += bloomFilterIndex.getSizeInBytes();
        }
        if (stripeRgNum != null) {
            size += (long) stripeRgNum.size() * Integer.BYTES;
        }
        this.estimatedSizeInBytes = size;
    }
}
