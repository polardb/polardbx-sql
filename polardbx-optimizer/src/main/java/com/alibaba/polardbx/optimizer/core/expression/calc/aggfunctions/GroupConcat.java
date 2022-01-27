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

package com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.net.util.CharsetUtil;
import com.alibaba.polardbx.optimizer.chunk.BlockBuilder;
import com.alibaba.polardbx.optimizer.chunk.Chunk;
import com.alibaba.polardbx.optimizer.core.CursorMeta;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.expression.calc.AbstractAggregator;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Created by zilin.zl on 18/10/17.
 */
public class GroupConcat extends AbstractAggregator {

    private HashMap<Integer, StringBuilder> stringBuilders = new HashMap<>();
    private String separator = ",";
    private List<Integer> aggOrderIndexList;
    private HashMap<Integer, ArrayList<Row>> tempLists = new HashMap<>();
    private int maxLen = 1024;
    private String encoding = "utf8";
    private Set<Integer> groupHasAppendToString = new HashSet<>();
    private int[] inputColumnIndexes;
    private List<Boolean> isAscList;
    private MemoryAllocatorCtx memoryAllocator;

    public GroupConcat(int[] aggTargetIndexes, boolean isDistinct, String separator,
                       List<Integer> aggOrderIndexList, List<Boolean> isAscList, int maxLen, String encoding,
                       MemoryAllocatorCtx allocator, int filterArg, DataType outputType) {
        super(aggTargetIndexes, isDistinct, null, outputType, filterArg);
        this.aggOrderIndexList = aggOrderIndexList;
        this.memoryAllocator = allocator;
        if (separator != null) {
            this.separator = separator;
        }
        this.isAscList = isAscList;
        this.maxLen = maxLen;
        if (encoding != null && encoding.length() != 0) {
            this.encoding = encoding;
        }

        inputColumnIndexes = buildInputColumnIndexes();
    }

    @Override
    public void accumulate(int groupId, Chunk chunk, int position) {
        Chunk.ChunkRow row = chunk.rowAt(position);
        Row arrayRow = FunctionUtils.fromIRowSetToArrayRowSet(row);
        boolean containsNull = Arrays.stream(aggIndexInChunk).anyMatch(i -> arrayRow.getObject(i) == null);
        if (containsNull) {
            return;
        }
        if (needSort()) {
            memoryAllocator.allocateReservedMemory(arrayRow);
            ArrayList<Row> tempList = tempLists.computeIfAbsent(groupId, k -> new ArrayList<>());
            tempList.add(arrayRow);
        } else {
            appendToStringBuilder(groupId, row);
        }
    }

    @Override
    public void writeResultTo(int groupId, BlockBuilder bb) {
        if (needSort()) {
            sortTempList(groupId);
            tempListAppendToStringBuilder(groupId);
        }
        StringBuilder stringBuilder = stringBuilders.get(groupId);
        if (stringBuilder == null) {
            bb.appendNull();
            return;
        }
        String result = stringBuilder.toString();
        if (result.length() > maxLen) {
            bb.writeByteArray(result.substring(0, maxLen).getBytes());
        } else {
            bb.writeByteArray(result.getBytes());
        }
    }

    @Override
    public void open(int capacity) {
        resetToInitValue(0);
    }

    @Override
    public void resetToInitValue(int groupId) {
        stringBuilders = new HashMap<>();
        groupHasAppendToString = new HashSet<>();
        for (ArrayList<Row> list : tempLists.values()) {
            list.forEach(t -> memoryAllocator.releaseReservedMemory(t.estimateSize(), true));
        }
        tempLists = new HashMap<>();
    }

    private boolean needSort() {
        return aggOrderIndexList != null && aggOrderIndexList.size() != 0;
    }

    private void sortTempList(int groupId) {
        ArrayList<Row> tempList = tempLists.get(groupId);
        if (tempList == null) {
            return;
        }
        tempList.sort(new Comparator<Row>() {

            @Override
            public int compare(Row r1, Row r2) {
                if (r1 == null || r2 == null) {
                    GeneralUtil.nestedException("Memory is insufficient to execute this query");
                }
                for (int j = 0; j < aggOrderIndexList.size(); j++) {
                    Integer i = aggOrderIndexList.get(j);
                    int v = isAscList.get(j) ? 1 : -1;
                    if (r1.getObject(i) == null) {
                        return v;
                    }
                    if (r2.getObject(i) == null) {
                        return -v;
                    }
                    DataType dataType = DataTypeUtil.getTypeOfObject(r1.getObject(i));
                    int result = dataType.compare(r1.getObject(i), r2.getObject(i));
                    if (result == 0) {
                        continue;
                    } else {
                        return v * result;
                    }
                }
                return 0; // equal
            }
        });
    }

    private void tempListAppendToStringBuilder(int groupId) {
        ArrayList<Row> tempList = tempLists.get(groupId);
        if (tempList == null) {
            return;
        }
        for (int i = 0; i < tempList.size(); i++) {
            Row row = tempList.get(i);
            if (row == null) {
                GeneralUtil.nestedException("Memory is insufficient to execute this query");
            }
            appendToStringBuilder(groupId, row);
        }
    }

    private void appendToStringBuilder(int groupId, Row row) {
        StringBuilder stringBuilder = stringBuilders.get(groupId);
        if (stringBuilder == null) {
            stringBuilder = new StringBuilder();
            stringBuilders.put(groupId, stringBuilder);
        }
        if (stringBuilder.length() > maxLen) {
            stringBuilder.setLength(maxLen);
            return;
        }
        if (groupHasAppendToString.contains(groupId)) {
            stringBuilder.append(separator);
        } else {
            groupHasAppendToString.add(groupId);
        }
        for (int targetIndex : aggIndexInChunk) {
            Object o = row.getObject(targetIndex);
            DataType dataType = DataTypeUtil.getTypeOfObject(o);
            if (DataTypeUtil.equalsSemantically(dataType, DataTypes.BytesType)) {
                CursorMeta meta = row.getParentCursorMeta();
                if (meta != null
                    && DataTypeUtil
                    .equalsSemantically(meta.getColumnMeta(targetIndex).getDataType(), DataTypes.BitType)) {
                    stringBuilder.append(row.getLong(targetIndex));
                } else { // Binary Type
                    stringBuilder.append(encodeByteArray(row.getBytes(targetIndex)));
                }
            } else if (DataTypeUtil.anyMatchSemantically(dataType, DataTypes.TimestampType, DataTypes.DatetimeType)) {
                String timeStampString = timeStampToString(DataTypes.TimestampType.convertFrom(o));
                int size = timeStampString.length();
                String padding = "000000000";
                CursorMeta meta = row.getParentCursorMeta();
                DataType columnType = null;
                if (meta != null && meta.getColumnMeta(targetIndex) != null) {
                    columnType = meta.getColumnMeta(targetIndex).getDataType();
                }

                if (DataTypeUtil.anyMatchSemantically(columnType, DataTypes.TimestampType, DataTypes.DatetimeType)) {
                    size = (int) meta.getColumnMeta(targetIndex).getField().getLength();
                }
                timeStampString += padding;
                stringBuilder.append(timeStampString.substring(0, size));
            } else if (DataTypeUtil.equalsSemantically(dataType, DataTypes.BooleanType)) {
                stringBuilder.append(booleanToString(row.getBoolean(targetIndex)));
            } else {
                stringBuilder.append(DataTypes.StringType.convertFrom(o));
            }
        }
    }

    private String encodeByteArray(byte[] b) {
        String s;
        try {
            s = new String(b, CharsetUtil.getJavaCharset(encoding));
        } catch (UnsupportedEncodingException e) {
            s = new String(b, StandardCharsets.UTF_8);
        }
        return s;
    }

    private String timeStampToString(Timestamp timestamp) {
        SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US); //$NON-NLS-1$
        StringBuffer buf = new StringBuffer();
        buf.append(dateFormatter.format(timestamp));
        int nanos = timestamp.getNanos();
        buf.append('.');
        buf.append(TStringUtil.formatNanos(nanos, true));
        return buf.toString();
    }

    private String booleanToString(Boolean b) {
        return b ? "1" : "0";
    }

    @Override
    public int[] getInputColumnIndexes() {
        return inputColumnIndexes;
    }

    private int[] buildInputColumnIndexes() {
        int[] inputColumnIndexes;
        if (needSort()) {
            inputColumnIndexes = new int[originTargetIndexes.length + aggOrderIndexList.size()];
            for (int i = 0; i < originTargetIndexes.length; i++) {
                inputColumnIndexes[i] = originTargetIndexes[i];
            }
            for (int i = 0; i < aggOrderIndexList.size(); i++) {
                inputColumnIndexes[originTargetIndexes.length + i] = aggOrderIndexList.get(i);
            }
        } else {
            inputColumnIndexes = originTargetIndexes;
        }
        return inputColumnIndexes;
    }

    public static int[] toIntArray(List<Integer> integers) {
        int[] result = new int[integers.size()];
        for (int i = 0; i < integers.size(); i++) {
            result[i] = integers.get(i);
        }
        return result;
    }

    @Override
    public void setAggIndexInChunk(int[] aggIndexInChunk) {
        super.setAggIndexInChunk(aggIndexInChunk);
        List<Integer> newAggOrderIndexList;
        if (needSort()) {
            newAggOrderIndexList = new ArrayList<>();
            for (int i = 0; i < aggOrderIndexList.size(); i++) {
                newAggOrderIndexList.add(aggIndexInChunk.length + i);
            }
        } else {
            newAggOrderIndexList = null;
        }
        aggOrderIndexList = newAggOrderIndexList;
    }
}
