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

package com.alibaba.polardbx.executor.test.corona.test;

import com.google.common.collect.ImmutableList;
import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.executor.cursor.AbstractCursor;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.MergeSortCursor;
import com.alibaba.polardbx.executor.utils.OrderByOption;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import org.apache.calcite.rel.RelFieldCollation;
import org.junit.Test;
import testframework.testbase.ExecutorTestBase;

import java.util.List;

/**
 * Created by chuanqin on 17/10/19.
 */
public class MergeSortCursorTest {

    private static class SortByOne extends TestMergeSortExecutorBase {

        @Override
        protected List<DataType> prepareColDataType() {
            return ImmutableList.of(DataTypes.IntegerType);
        }

        @Override
        protected List<OrderByOption> prepareOrderBy() {
            // TODO: null direction modification remains unimplemented
            return ImmutableList.of(new OrderByOption(0,
                RelFieldCollation.Direction.ASCENDING,
                RelFieldCollation.NullDirection.LAST));
        }
    }

    private static class SortByOneAndTwo extends TestMergeSortExecutorBase {

        @Override
        protected List<DataType> prepareColDataType() {
            return ImmutableList.of(DataTypes.IntegerType, DataTypes.StringType);
        }

        @Override
        protected List<OrderByOption> prepareOrderBy() {
            // TODO: null direction modification remains unimplemented
            return ImmutableList.of(
                new OrderByOption(0,
                    RelFieldCollation.Direction.ASCENDING,
                    RelFieldCollation.NullDirection.LAST),
                new OrderByOption(1,
                    RelFieldCollation.Direction.DESCENDING,
                    RelFieldCollation.NullDirection.LAST));
        }

    }

    private SortByOne sortByOne = new SortByOne();
    private SortByOneAndTwo sortByOneTwo = new SortByOneAndTwo();

    @Test
    public void testSortInteger1() {
        sortByOne.setInput1(ImmutableList.of(new Integer[] {1}, new Integer[] {3}, new Integer[] {5}));
        sortByOne.setInput2(ImmutableList.of(new Integer[] {2}, new Integer[] {5}, new Integer[] {6}));
        sortByOne.setInputColumns(ImmutableList.of(DataTypes.IntegerType));

        sortByOne.setRef(ImmutableList.of(new Integer[] {1},
            new Integer[] {2},
            new Integer[] {3},
            new Integer[] {5},
            new Integer[] {5},
            new Integer[] {6}));
        sortByOne.validateResult();
    }

    @Test
    public void testSortInteger2() {
        sortByOne.setInput1(ImmutableList.of(new Integer[] {null}, new Integer[] {2}, new Integer[] {3}));
        sortByOne.setInput2(ImmutableList.of(new Integer[] {2}, new Integer[] {5}, new Integer[] {6}));
        sortByOne.setInputColumns(ImmutableList.of(DataTypes.IntegerType));

        sortByOne.setRef(ImmutableList.of(new Integer[] {null},
            new Integer[] {2},
            new Integer[] {2},
            new Integer[] {3},
            new Integer[] {5},
            new Integer[] {6}));
        sortByOne.validateResult();
    }

    @Test
    public void testSortIntegerEmpty() {
        sortByOne.setInput1(ImmutableList.of());
        sortByOne.setInput2(ImmutableList.of());
        sortByOne.setInputColumns(ImmutableList.of(DataTypes.IntegerType));
        sortByOne.setRef(ImmutableList.of());
        sortByOne.validateResult();
    }

    @Test
    public void testSortDecimal() {
        sortByOne.setInput1(ImmutableList.of(new Object[] {Decimal.fromString("-10.5")},
            new Object[] {Decimal.fromString("8.8")},
            new Object[] {Decimal.fromString("100.5")}));
        sortByOne.setInput2(ImmutableList.of(new Object[] {Decimal.fromString("-10.5")},
            new Object[] {Decimal.fromString("8.8")},
            new Object[] {Decimal.fromString("111")}));
        sortByOne.setInputColumns(ImmutableList.of(DataTypes.DecimalType));

        sortByOne.setRef(ImmutableList.of(new Object[] {Decimal.fromString("-10.5")},
            new Object[] {Decimal.fromString("-10.5")},
            new Object[] {Decimal.fromString("8.8")},
            new Object[] {Decimal.fromString("8.8")},
            new Object[] {Decimal.fromString("100.5")},
            new Object[] {Decimal.fromString("111")}));
        sortByOne.setColumnDataTypeList(ImmutableList.of(DataTypes.DecimalType));
        sortByOne.validateResult();
    }

    @Test
    public void testSortIntegerString1() {
        sortByOneTwo
            .setInput1(ImmutableList.of(new Object[] {2, "c"}, new Object[] {3, "bcd"}, new Object[] {4, "ab"}));
        sortByOneTwo
            .setInput2(ImmutableList.of(new Object[] {3, "d"}, new Object[] {4, "bcd"}, new Object[] {5, "ab"}));
        sortByOneTwo.setInputColumns(ImmutableList.of(DataTypes.IntegerType, DataTypes.StringType));

        sortByOneTwo.setRef(ImmutableList.of(new Object[] {2, "c"},
            new Object[] {3, "d"},
            new Object[] {3, "bcd"},
            new Object[] {4, "bcd"},
            new Object[] {4, "ab"},
            new Object[] {5, "ab"}));
        sortByOneTwo.validateResult();
    }

    private abstract static class TestMergeSortExecutorBase extends ExecutorTestBase {

        @Override
        protected AbstractCursor buildTestCursor() {
            List<Cursor> inputs = ImmutableList.of(inputCursor1, inputCursor2);
            return new MergeSortCursor(null, inputs, prepareOrderBy(), 0, Long.MAX_VALUE);
        }

        protected abstract List<OrderByOption> prepareOrderBy();
    }

}
