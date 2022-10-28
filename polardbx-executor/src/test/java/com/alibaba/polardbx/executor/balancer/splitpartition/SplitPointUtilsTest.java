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

package com.alibaba.polardbx.executor.balancer.splitpartition;

import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.VarcharType;
import com.alibaba.polardbx.optimizer.partition.PartitionByDefinition;
import com.alibaba.polardbx.optimizer.partition.PartitionStrategy;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionField;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionFieldBuilder;
import com.alibaba.polardbx.optimizer.partition.datatype.function.PartitionIntFunction;
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumHasher;
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumInfo;
import io.airlift.slice.Slice;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * @author moyi
 * @since 2021/04
 */
public class SplitPointUtilsTest {

    @Test
    public void testBuildSplitPoint() {
        List<Integer> splitPoints = new ArrayList<>();
        int splitSize = 10;
        int counts[] = {4, 10, 12, 7, 3, 12, 9, 21, 3, 6, 8, 19, 7, 1, 19};
        List<Integer> data = new ArrayList<>();
        for (int i = 0; i < counts.length; i++) {
            for (int j = 0; j < counts[i]; j++) {
                data.add(i);
            }
        }
        int lastRow = 0;
        int row = lastRow + splitSize;
        while (true) {
            int comparedValue = data.get(row);
            int value = data.get(row);
            int j = row;
            for (; j > lastRow; j--) {
                value = data.get(j);
                if (comparedValue != value) {
                    j = j + 1;
                    break;
                }
            }
            if (j == lastRow) {
                for (j = row; j < data.size(); j++) {
                    value = data.get(j);
                    if (comparedValue != value) {
                        break;
                    }
                }
            }
            lastRow = j;
            if (lastRow >= data.size()) {
                break;
            }
            splitPoints.add(data.get(lastRow));
            if (lastRow + splitSize >= data.size() - 1) {
                break;
            }
            row = lastRow + splitSize;
        }
        int rightSplitPoints[] = {1, 2, 3, 5, 6, 7, 8, 10, 11, 12, 14};
        for (int i = 0; i < rightSplitPoints.length; i++) {
            assert (rightSplitPoints[i] == splitPoints.get(i));
        }

    }

    //    @Test
    public void testBuildSplitBound() {
        // single column
        DataType<Slice> stringType = new VarcharType();
        PartitionField field = PartitionFieldBuilder.createField(stringType);
        field.store("2021-01-01", stringType);
        SearchDatumInfo tuple = SearchDatumInfo.createFromField(field);

        // multi column
        SearchDatumInfo tuple2 = SearchDatumInfo.createFromFields(Arrays.asList(field, field));

        PartitionIntFunction yearFunc = PartitionIntFunction.create(TddlOperatorTable.YEAR);

        SearchDatumHasher hasher = new SearchDatumHasher();
        PartitionByDefinition partitionBy = new PartitionByDefinition();
        partitionBy.setPartIntFunc(yearFunc);
        partitionBy.setHasher(hasher);

        SearchDatumInfo hashedResult = SearchDatumInfo.createFromHashCode(-585273832481696521L);
        SearchDatumInfo originResult = SearchDatumInfo.createFromField(field);
        SearchDatumInfo yearResult = SearchDatumInfo.createFromHashCode(2021);
        SearchDatumInfo hashedYearResult = SearchDatumInfo.createFromHashCode(54516667770174486L);
        SearchDatumInfo result4 =
            SearchDatumInfo.createFromHashCodes(new Long[] {-585273832481696521L, -585273832481696521L});

        List<SplitBoundTestCase> testCases = Arrays.asList(
            // without function
            new SplitBoundTestCase(PartitionStrategy.KEY, null, tuple, hashedResult),
            new SplitBoundTestCase(PartitionStrategy.RANGE_COLUMNS, null, tuple, originResult),

            // with function
            new SplitBoundTestCase(PartitionStrategy.HASH, yearFunc, tuple, hashedYearResult),
            new SplitBoundTestCase(PartitionStrategy.RANGE, yearFunc, tuple, yearResult),

            // multi column partition
            new SplitBoundTestCase(PartitionStrategy.KEY, null, tuple2, result4),
            new SplitBoundTestCase(PartitionStrategy.RANGE, null, tuple2, tuple2),
            new SplitBoundTestCase(PartitionStrategy.RANGE_COLUMNS, null, tuple2, tuple2)

        );

        for (int i = 0; i < testCases.size(); i++) {
            SplitBoundTestCase t = testCases.get(i);
            partitionBy.setPartIntFunc(t.func);
            partitionBy.setStrategy(t.strategy);

            SearchDatumInfo result = SplitPointUtils.generateSplitBound(partitionBy, t.input);
            Assert.assertEquals(String.format("case %d %s", i, t), t.expectedResult, result);
        }

        String schema = "test_split_partition";
        String tableName = "t1";
        String partName = "p1";
        int splitCount = 10;
        ExecutionContext ec = new ExecutionContext();
        ec.setParams(new Parameters());
        ec.setSchemaName(schema);
        ec.setServerVariables(new HashMap<>());
        for (int i = 0; i < testCases.size(); i++) {
            SplitBoundTestCase t = testCases.get(i);
            partitionBy.setPartIntFunc(t.func);
            partitionBy.setStrategy(t.strategy);

            List<SearchDatumInfo> result =
                SplitPointUtils.generateSplitBounds(schema, tableName, partName, splitCount, -1L);
            Assert.assertEquals(String.format("case %d %s", i, t), t.expectedResult, result.get(0));
        }
    }

    class SplitBoundTestCase {
        PartitionStrategy strategy;
        PartitionIntFunction func;
        SearchDatumInfo input;

        SearchDatumInfo expectedResult;

        public SplitBoundTestCase(PartitionStrategy strategy,
                                  PartitionIntFunction func,
                                  SearchDatumInfo tuple,
                                  SearchDatumInfo expectedResult) {
            this.strategy = strategy;
            this.func = func;
            this.input = tuple;
            this.expectedResult = expectedResult;
        }

    }

}
