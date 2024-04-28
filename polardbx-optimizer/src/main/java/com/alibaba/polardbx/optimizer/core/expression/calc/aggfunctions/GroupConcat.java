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
import com.alibaba.polardbx.optimizer.core.CursorMeta;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.expression.IFunction;
import com.alibaba.polardbx.optimizer.core.expression.calc.Aggregator;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.memory.MemoryAllocatorCtx;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;
import org.apache.calcite.sql.SqlKind;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;

import static org.apache.calcite.sql.SqlKind.GROUP_CONCAT;

/**
 * Created by zilin.zl on 18/10/17.
 */
public class GroupConcat extends Aggregator {

    private StringBuilder stringBuilder;
    private String separator = ",";
    private List<Integer> aggOrderIndexList;
    private ArrayList<Row> tempList;
    private int maxLen = 1024;
    private String encoding = "utf8";
    private boolean firstAppendToStringBuilder = true;
    // inputColumnIndxes = aggTargetIndexes + aggOrderIndexList
    private int[] inputColumnIndexes;
    private List<Boolean> isAscList;

    public GroupConcat() {
    }

    public GroupConcat(int[] aggTargetIndexes, boolean isDistinct, String separator,
                       List<Integer> aggOrderIndexList, List<Boolean> isAscList, int maxLen, String encoding,
                       MemoryAllocatorCtx allocator, int filterArg) {
        super(aggTargetIndexes, isDistinct, allocator, filterArg);
        this.aggOrderIndexList = aggOrderIndexList;
        if (separator != null) {
            this.separator = separator;
        }
        this.isAscList = isAscList;
        tempList = new ArrayList<>();
        this.maxLen = maxLen;
        if (encoding != null && encoding.length() != 0) {
            this.encoding = encoding;
        }

        inputColumnIndexes = buildInputColumnIndexes();
    }

    @Override
    public SqlKind getSqlKind() {
        return GROUP_CONCAT;
    }

    @Override
    protected void conductAgg(Object row) {
        if (needSort()) {
            Row arrayRow = FunctionUtils.fromIRowSetToArrayRowSet((Row) row);
            memoryAllocator.allocateReservedMemory(arrayRow);
            tempList.add(arrayRow);
        } else {
            appendToStringBuilder((Row) row);
        }
    }

    @Override
    public GroupConcat getNew() {
        return new GroupConcat(aggTargetIndexes,
            isDistinct,
            separator,
            aggOrderIndexList,
            isAscList,
            maxLen,
            encoding,
            memoryAllocator,
            filterArg);
    }

    @Override
    public Object eval(Row row) {
        if (needSort()) {
            sortTempList();
            tempListAppendToStringBuilder();
        }
        if (stringBuilder == null) {
            return null;
        }
        String result = stringBuilder.toString();
        if (result.length() > maxLen) {
            return result.substring(0, maxLen);
        } else {
            return result;
        }
    }

    private boolean needSort() {
        return aggOrderIndexList != null && aggOrderIndexList.size() != 0;
    }

    private void sortTempList() {
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

    private void tempListAppendToStringBuilder() {
        for (int i = 0; i < tempList.size(); i++) {
            Row row = tempList.get(i);
            if (row == null) {
                GeneralUtil.nestedException("Memory is insufficient to execute this query");
            }
            appendToStringBuilder(row);
        }
    }

    private void appendToStringBuilder(Row row) {
        if (stringBuilder == null) {
            stringBuilder = new StringBuilder();
        }
        if (stringBuilder.length() > maxLen) {
            stringBuilder.setLength(maxLen);
            return;
        }
        if (!firstAppendToStringBuilder) {
            stringBuilder.append(separator);
        } else {
            firstAppendToStringBuilder = false;
        }
        for (int targetIndex : aggTargetIndexes) {
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
    public void setFunction(IFunction function) {

    }

    @Override
    public IFunction.FunctionType getFunctionType() {
        return IFunction.FunctionType.Aggregate;
    }

    @Override
    public Object compute() {
        return null;
    }

    @Override
    public DataType getReturnType() {
        return resultField.getDataType();
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"GROUP_CONCAT_V2"};
    }

    @Override
    public void clear() {

    }

    @Override
    public int getScale() {
        return 0;
    }

    @Override
    public int getPrecision() {
        return 0;
    }

    @Override
    public int[] getInputColumnIndexes() {
        return inputColumnIndexes;
    }

    private int[] buildInputColumnIndexes() {
        int[] inputColumnIndexes;
        if (needSort()) {
            inputColumnIndexes = new int[aggTargetIndexes.length + aggOrderIndexList.size()];
            for (int i = 0; i < aggTargetIndexes.length; i++) {
                inputColumnIndexes[i] = aggTargetIndexes[i];
            }
            for (int i = 0; i < aggOrderIndexList.size(); i++) {
                inputColumnIndexes[aggTargetIndexes.length + i] = aggOrderIndexList.get(i);
            }

        } else {
            inputColumnIndexes = aggTargetIndexes;
        }
        return inputColumnIndexes;
    }

    @Override
    public Aggregator getNewForAccumulator() {
        // change targetIndexes to be compatible with accumulator inputChunk
        int[] newAggTargetIndexes = new int[aggTargetIndexes.length];
        for (int i = 0; i < aggTargetIndexes.length; i++) {
            newAggTargetIndexes[i] = i;
        }

        List<Integer> newAggOrderIndexList;
        if (needSort()) {
            newAggOrderIndexList = new ArrayList<>();
            for (int i = 0; i < aggOrderIndexList.size(); i++) {
                newAggOrderIndexList.add(aggTargetIndexes.length + i);
            }
        } else {
            newAggOrderIndexList = null;
        }

        return new GroupConcat(newAggTargetIndexes,
            false,  // accumulator do not need aggregator to do distinct by itself
            separator,
            newAggOrderIndexList,
            isAscList,
            maxLen,
            encoding,
            memoryAllocator,
            filterArg);
    }

    public static int[] toIntArray(List<Integer> integers) {
        int[] result = new int[integers.size()];
        for (int i = 0; i < integers.size(); i++) {
            result[i] = integers.get(i);
        }
        return result;
    }
}
