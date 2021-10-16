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

package testframework.testbase;

import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.utils.convertor.Convertor;
import com.alibaba.polardbx.common.utils.convertor.ConvertorHelper;
import com.alibaba.polardbx.executor.cursor.AbstractCursor;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.Field;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.memory.MemoryManager;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by chuanqin on 17/9/26.
 */
public abstract class ExecutorTestBase {

    protected final ExecutionContext context;

    {
        context = new ExecutionContext();
        context.setMemoryPool(MemoryManager.getInstance().getGlobalMemoryPool().getOrCreatePool("Test"));
        context.setParams(new Parameters());
    }

    protected AbstractCursor testCursor;
    protected AbstractCursor inputCursor1;
    protected AbstractCursor inputCursor2;

    protected List<Object[]> input1;
    protected List<Object[]> input2;
    protected List<ColumnMeta> columns;

    protected List<Object[]> ref;

    protected List<DataType> columnDataTypeList;

    private ConvertorHelper convertorHelper = ConvertorHelper.getInstance();

    protected void init() {
        try {
            buildInput();
        } catch (Exception e) {
            e.printStackTrace();
        }
        testCursor = buildTestCursor();
        buildReference();
    }

    protected void buildInput() throws Exception {
        if (input1 != null) {
            inputCursor1 = new ListMockedCursor(input1, columns);
            inputCursor2 = new ListMockedCursor(input2, columns);
        } else {
            if (prepareInput1() == null) {
                throw new Exception("Illegal input");
            }
            inputCursor1 = new ListMockedCursor(prepareInput1(), columns);
            inputCursor2 = new ListMockedCursor(prepareInput2(), columns);
        }
    }

    protected abstract AbstractCursor buildTestCursor();

    protected void buildReference() {
        if (ref == null) {
            ref = prepareRef();
        }
    }

    protected List<Object[]> prepareRef() {
        return null;
    }

    /**
     * build input 1, used for single input cursor
     */
    protected List<Object[]> prepareInput1() {
        return null;
    }

    /**
     * build input 2, used for bi-input cursor
     */
    protected List<Object[]> prepareInput2() {
        return null;
    }

    /**
     * Validate the correctness of result generated from cursor that is tested.
     */
    public void validateResult() {
        init();
        List<Object> testCursorResult = new ArrayList<>();
        Row curRow;
        while ((curRow = testCursor.next()) != null) {
            testCursorResult.add(covertRowToReturnType(curRow));
        }
        Assert.assertArrayEquals(ref.toArray(), testCursorResult.toArray());
    }

    public void setInput1(List<Object[]> input1) {
        this.input1 = input1;
    }

    public void setInput2(List<Object[]> input2) {
        this.input2 = input2;
    }

    public void setInputColumns(List<DataType> columnTypes) {
        List<ColumnMeta> columns = new ArrayList<>(columnTypes.size());
        for (int i = 0; i < columnTypes.size(); i++) {
            String columnName = "COLUMN_" + i;
            Field field = new Field("MOCK_TABLE", columnName, columnTypes.get(i));
            ColumnMeta column = new ColumnMeta("MOCK_TABLE", columnName, null, field);
            columns.add(column);
        }
        this.columns = columns;
    }

    public void setRef(List<Object[]> ref) {
        this.ref = ref;
    }

    private Object[] covertRowToReturnType(Row test) {
        List<Object> testValues = test.getValues();
        Object[] convertedRow = new Object[testValues.size()];
        for (int i = 0; i < testValues.size(); i++) {
            if (testValues.get(i) != null) {
                Class destClass = testCursor.getReturnColumns().get(i).getDataType().getDataClass();
                Convertor convertor = null;
                Object convertedValue = testValues.get(i);
                if (testValues.get(i) != null) {
                    convertor = convertorHelper.getConvertor(testValues.get(i).getClass(), destClass);
                }
                if (convertor != null) {
                    convertedValue = convertor.convert(testValues.get(i), destClass);
                }
                convertedRow[i] = convertedValue;
            }
        }
        return convertedRow;
    }

    /**
     * Build meta info for cursor
     */
    protected List<ColumnMeta> buildColumnMeta(List<DataType> dataTypes) {
        List<ColumnMeta> columnMetas = new ArrayList<>();
        for (DataType dataType : dataTypes) {
            columnMetas.add(new ColumnMeta("test", "test", "test", new Field(dataType)));
        }
        return columnMetas;
    }

    public void testIt() {

        validateResult();
        System.out.println("succeeded");
    }

    /**
     * Set data type
     */
    protected List<DataType> prepareColDataType() {
        return null;
    }

    public void setColumnDataTypeList(List<DataType> columnDataTypeList) {
        this.columnDataTypeList = columnDataTypeList;
    }

}
