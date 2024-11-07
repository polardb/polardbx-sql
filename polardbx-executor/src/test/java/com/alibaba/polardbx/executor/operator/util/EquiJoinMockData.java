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

package com.alibaba.polardbx.executor.operator.util;

import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.IntegerBlock;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.chunk.StringBlock;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.join.EquiJoinKey;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

/**
 * @author <a href="mailto:shitianshuo.sts@alibaba-inc.com"></a>
 */
public class EquiJoinMockData {

    private static final List<DataType> INT_INNER_TYPES =
        ImmutableList.of(DataTypes.IntegerType);
    private static final List<DataType> LONG_INNER_TYPES =
        ImmutableList.of(DataTypes.LongType);
    private static final List<DataType> LONG_INT_INNER_TYPES =
        ImmutableList.of(DataTypes.LongType, DataTypes.IntegerType);
    private static final List<DataType> INT_INT_TYPES =
        ImmutableList.of(DataTypes.IntegerType, DataTypes.IntegerType);
    private static final List<DataType> INT_OUTER_TYPES =
        ImmutableList.of(DataTypes.IntegerType, DataTypes.IntegerType);
    private static final List<DataType> LONG_OUTER_TYPES =
        ImmutableList.of(DataTypes.LongType, DataTypes.LongType);
    private static final List<DataType> LONG_INT_OUTER_TYPES =
        ImmutableList.of(DataTypes.LongType, DataTypes.IntegerType);
    private static final List<DataType> INT_INT_OUTER_TYPES =
        ImmutableList.of(DataTypes.IntegerType, DataTypes.IntegerType);
    private static final List<DataType> LONG_INT_INT_TYPES =
        ImmutableList.of(DataTypes.LongType, DataTypes.IntegerType, DataTypes.IntegerType);
    private static final List<DataType> LONG_INT_LONG_TYPES =
        ImmutableList.of(DataTypes.LongType, DataTypes.IntegerType, DataTypes.LongType);
    private static final List<DataType> LONG_LONG_INT_LONG_TYPES =
        ImmutableList.of(DataTypes.LongType, DataTypes.LongType, DataTypes.IntegerType, DataTypes.LongType);
    private static final List<DataType> LONG_LONG_LONG_TYPES =
        ImmutableList.of(DataTypes.LongType, DataTypes.LongType, DataTypes.LongType);
    private static final List<DataType> LONG_LONG_LONG_LONG_TYPES =
        ImmutableList.of(DataTypes.LongType, DataTypes.LongType, DataTypes.LongType, DataTypes.LongType);
    private static final List<DataType> ANTI_INT_NOT_EQ_INT_INNER_TYPES =
        ImmutableList.of(DataTypes.LongType, DataTypes.IntegerType, DataTypes.LongType, DataTypes.IntegerType);
    private static final List<DataType> ANTI_INT_NOT_EQ_INT_OUTER_TYPES =
        ImmutableList.of(DataTypes.IntegerType, DataTypes.LongType, DataTypes.IntegerType);
    private static final List<DataType> ANTI_LONG_NOT_EQ_INT_INNER_TYPES =
        ImmutableList.of(DataTypes.LongType, DataTypes.LongType, DataTypes.LongType, DataTypes.IntegerType,
            DataTypes.LongType);
    private static final List<DataType> ANTI_LONG_NOT_EQ_INT_OUTER_TYPES =
        ImmutableList.of(DataTypes.LongType, DataTypes.LongType, DataTypes.IntegerType, DataTypes.LongType);
    public static EquiJoinMockData INNER_INT_CASE = new EquiJoinMockData(
        INT_INNER_TYPES,
        new RowChunksBuilder(INT_INNER_TYPES)
            .addChunk(new Chunk(
                IntegerBlock.of(1, 2, 3, 4)))
            .addChunk(new Chunk(
                IntegerBlock.of(3, 5, 6, 7)))
            .build(),
        INT_OUTER_TYPES,
        new RowChunksBuilder(INT_OUTER_TYPES)
            .addChunk(new Chunk(
                IntegerBlock.of(0, 1, 2, 3),
                IntegerBlock.of(3, 4, 9, 7)))
            .addChunk(new Chunk(
                IntegerBlock.of(4, 5, 6, 7),
                IntegerBlock.of(5, 3, 8, 0)))
            .build(),
        ImmutableList.of(0),
        ImmutableList.of(1));

    public static EquiJoinMockData SEMI_CASE = new EquiJoinMockData(
        INT_INNER_TYPES,
        new RowChunksBuilder(INT_INNER_TYPES)
            .addChunk(new Chunk(
                IntegerBlock.of(1, 2, 3, 4)))
            .addChunk(new Chunk(
                IntegerBlock.of(3, 4, 5, 6)))
            .build(),
        INT_OUTER_TYPES,
        new RowChunksBuilder(INT_OUTER_TYPES)
            .addChunk(new Chunk(
                IntegerBlock.of(0, 1, 2, 3),
                IntegerBlock.of(3, 4, 9, 7)))
            .addChunk(new Chunk(
                IntegerBlock.of(4, 5, 6, 7),
                IntegerBlock.of(5, 3, 8, null)))
            .build(),
        ImmutableList.of(0),
        ImmutableList.of(1));

    public static EquiJoinMockData SEMI_CASE_2 = new EquiJoinMockData(
        INT_INNER_TYPES,
        new RowChunksBuilder(INT_INNER_TYPES)
            .addChunk(new Chunk(
                IntegerBlock.of(1, 2, 3, 4)))
            .addChunk(new Chunk(
                IntegerBlock.of(3, 4, 5, 6, null, null)))
            .build(),
        INT_OUTER_TYPES,
        new RowChunksBuilder(INT_OUTER_TYPES)
            .addChunk(new Chunk(
                IntegerBlock.of(0, 1, 2, 3),
                IntegerBlock.of(3, 4, 9, 7)))
            .addChunk(new Chunk(
                IntegerBlock.of(4, 5, 6, 7, null),
                IntegerBlock.of(5, 3, 8, null, null)))
            .build(),
        ImmutableList.of(0),
        ImmutableList.of(1));

    public static EquiJoinMockData INT_JOIN_LONG_CASE = new EquiJoinMockData(
        INT_INT_TYPES,
        new RowChunksBuilder(INT_INT_TYPES)
            .addChunk(new Chunk(
                IntegerBlock.of(1, 2, 3, 4),
                IntegerBlock.of(4, 5, 6, 7)))
            .addChunk(new Chunk(
                IntegerBlock.of(3, 4, 5, 6),
                IntegerBlock.of(4, 5, 6, 7)))
            .build(),
        LONG_INT_OUTER_TYPES,
        new RowChunksBuilder(LONG_INT_OUTER_TYPES)
            .addChunk(new Chunk(
                LongBlock.of(3L, 4L, 9L, 7L),
                IntegerBlock.of(0, 1, 2, 3)))
            .addChunk(new Chunk(
                LongBlock.of(5L, 3L, 8L, 10L),
                IntegerBlock.of(4, 5, 6, 7)))
            .build(),
        ImmutableList.of(0),
        ImmutableList.of(0));

    public static EquiJoinMockData SEMI_LONG_CASE = new EquiJoinMockData(
        LONG_INNER_TYPES,
        new RowChunksBuilder(LONG_INNER_TYPES)
            .addChunk(new Chunk(
                LongBlock.of(1L, 2L, 3L, 4L)))
            .addChunk(new Chunk(
                LongBlock.of(3L, 4L, 5L, 6L)))
            .build(),
        LONG_OUTER_TYPES,
        new RowChunksBuilder(LONG_OUTER_TYPES)
            .addChunk(new Chunk(
                LongBlock.of(0L, 1L, 2L, 3L),
                LongBlock.of(3L, 4L, 9L, 7L)))
            .addChunk(new Chunk(
                LongBlock.of(4L, 5L, 6L, 7L),
                LongBlock.of(5L, 3L, 8L, 10L)))
            .build(),
        ImmutableList.of(0),
        ImmutableList.of(1));

    public static EquiJoinMockData SEMI_LONG_NOT_EQ_INT_CASE = new EquiJoinMockData(
        LONG_LONG_INT_LONG_TYPES,
        new RowChunksBuilder(LONG_LONG_INT_LONG_TYPES)
            .addChunk(new Chunk(
                LongBlock.of(1L, 2L, 3L, 4L),
                LongBlock.of(3L, 4L, 5L, 6L),
                IntegerBlock.of(3, 4, 5, 6),
                LongBlock.of(-1L, -2L, -3L, -4L)))
            .addChunk(new Chunk(
                LongBlock.of(4L, 5L, 6L, 7L),
                LongBlock.of(7L, 8L, 9L, 10L),
                IntegerBlock.of(6, 7, 8, 9),
                LongBlock.of(-1L, -2L, -3L, -4L)))
            .build(),
        LONG_INT_LONG_TYPES,
        new RowChunksBuilder(LONG_INT_LONG_TYPES)
            .addChunk(new Chunk(
                LongBlock.of(0L, 1L, 3L, 3L),
                IntegerBlock.of(3, 4, 5, 7),
                LongBlock.of(11L, 12L, 13L, 14L)))
            .addChunk(new Chunk(
                LongBlock.of(4L, 5L, 6L, 7L),
                IntegerBlock.of(5, 7, 8, 10),
                LongBlock.of(15L, 16L, 17L, 18L)))
            .build(),
        ImmutableList.of(0),
        ImmutableList.of(0));

    public static EquiJoinMockData SEMI_LONG_NOT_EQ_LONG_CASE = new EquiJoinMockData(
        LONG_LONG_LONG_LONG_TYPES,
        new RowChunksBuilder(LONG_LONG_LONG_LONG_TYPES)
            .addChunk(new Chunk(
                LongBlock.of(1L, 2L, 3L, 4L),
                LongBlock.of(3L, 4L, 5L, 6L),
                LongBlock.of(3L, 4L, 5L, 6L),
                LongBlock.of(-1L, -2L, -3L, -4L)))
            .addChunk(new Chunk(
                LongBlock.of(4L, 5L, 6L, 7L),
                LongBlock.of(7L, 8L, 9L, 10L),
                LongBlock.of(6L, 7L, 8L, 9L),
                LongBlock.of(-1L, -2L, -3L, -4L)))
            .build(),
        LONG_LONG_LONG_TYPES,
        new RowChunksBuilder(LONG_LONG_LONG_TYPES)
            .addChunk(new Chunk(
                LongBlock.of(0L, 1L, 3L, 3L),
                LongBlock.of(3L, 4L, 5L, 7L),
                LongBlock.of(11L, 12L, 13L, 14L)))
            .addChunk(new Chunk(
                LongBlock.of(4L, 5L, 6L, 7L),
                LongBlock.of(5L, 7L, 8L, 10L),
                LongBlock.of(15L, 16L, 17L, 18L)))
            .build(),
        ImmutableList.of(0),
        ImmutableList.of(0));

    public static EquiJoinMockData SEMI_INT_NOT_EQ_INT_CASE = new EquiJoinMockData(
        LONG_INT_INT_TYPES,
        new RowChunksBuilder(LONG_INT_INT_TYPES)
            .addChunk(new Chunk(
                LongBlock.of(-1L, -2L, -3L, -4L),
                IntegerBlock.of(1, 2, 3, 4),
                IntegerBlock.of(3, 4, 5, 6)))
            .addChunk(new Chunk(
                LongBlock.of(-5L, -6L, -7L, -8L),
                IntegerBlock.of(4, 5, 6, 7),
                IntegerBlock.of(6, 7, 8, 9)))
            .build(),
        INT_INT_OUTER_TYPES,
        new RowChunksBuilder(INT_INT_OUTER_TYPES)
            .addChunk(new Chunk(
                IntegerBlock.of(0, 1, 3, 9),
                IntegerBlock.of(3, 4, 9, 7)))
            .addChunk(new Chunk(
                IntegerBlock.of(4, 5, 12, 11),
                IntegerBlock.of(5, 7, 8, 10)))
            .build(),
        ImmutableList.of(1),
        ImmutableList.of(0));

    public static EquiJoinMockData ANTI_INT_NOT_EQ_INT_CASE = new EquiJoinMockData(
        ANTI_INT_NOT_EQ_INT_INNER_TYPES,
        new RowChunksBuilder(ANTI_INT_NOT_EQ_INT_INNER_TYPES)
            .addChunk(new Chunk(
                LongBlock.of(-1L, -1L, -1L, -1L),
                IntegerBlock.of(1, 2, 3, 4),
                LongBlock.of(-3L, -3L, -3L, -3L),
                IntegerBlock.of(3, 4, 5, 6)))
            .addChunk(new Chunk(
                LongBlock.of(-2L, -2L, -2L, -2L),
                IntegerBlock.of(4, 5, 6, 7),
                LongBlock.of(-5L, -5L, -5L, -5L),
                IntegerBlock.of(6, 7, 8, 9)))
            .build(),
        ANTI_INT_NOT_EQ_INT_OUTER_TYPES,
        new RowChunksBuilder(ANTI_INT_NOT_EQ_INT_OUTER_TYPES)
            .addChunk(new Chunk(
                IntegerBlock.of(0, 1, 3, 9),
                LongBlock.of(-5L, -4L, -3L, -1L),
                IntegerBlock.of(3, 4, 9, 7)))
            .addChunk(new Chunk(
                IntegerBlock.of(4, 5, 12, 11),
                LongBlock.of(-9L, -8L, -7L, -6L),
                IntegerBlock.of(5, 7, 8, 10)))
            .build(),
        ImmutableList.of(1),
        ImmutableList.of(0));

    public static EquiJoinMockData ANTI_LONG_NOT_EQ_INT_CASE = new EquiJoinMockData(
        ANTI_LONG_NOT_EQ_INT_INNER_TYPES,
        new RowChunksBuilder(ANTI_LONG_NOT_EQ_INT_INNER_TYPES)
            .addChunk(new Chunk(
                LongBlock.of(1L, 2L, 3L, 4L),
                LongBlock.of(0L, 0L, 0L, 0L),
                LongBlock.of(-1L, -1L, -1L, -1L),
                IntegerBlock.of(3, 4, 5, 6),
                LongBlock.of(-1L, -1L, -1L, -1L)))
            .addChunk(new Chunk(
                LongBlock.of(4L, 5L, 6L, 7L),
                LongBlock.of(0L, 0L, 0L, 0L),
                LongBlock.of(-1L, -1L, -1L, -1L),
                IntegerBlock.of(6, 7, 8, 9),
                LongBlock.of(-1L, -1L, -1L, -1L)))
            .build(),
        ANTI_LONG_NOT_EQ_INT_OUTER_TYPES,
        new RowChunksBuilder(ANTI_LONG_NOT_EQ_INT_OUTER_TYPES)
            .addChunk(new Chunk(
                LongBlock.of(0L, 1L, 3L, 3L),
                LongBlock.of(-2L, -3L, -4L, -5L),
                IntegerBlock.of(3, 4, 5, 7),
                LongBlock.of(-5L, -6L, -7L, -8L)))
            .addChunk(new Chunk(
                LongBlock.of(4L, 5L, 6L, 7L),
                LongBlock.of(-1L, -2L, -3L, -4L),
                IntegerBlock.of(5, 7, 8, 10),
                LongBlock.of(-6L, -7L, -8L, -9L)))
            .build(),
        ImmutableList.of(0),
        ImmutableList.of(0));

    public static EquiJoinMockData REVERSE_SEMI_INT_CASE = new EquiJoinMockData(
        INT_INNER_TYPES,
        new RowChunksBuilder(INT_INNER_TYPES)
            .addChunk(new Chunk(
                IntegerBlock.of(1, 2, 3, 4)))
            .addChunk(new Chunk(
                IntegerBlock.of(3, 4, 5, 6)))
            .build(),
        INT_OUTER_TYPES,
        new RowChunksBuilder(INT_OUTER_TYPES)
            .addChunk(new Chunk(
                IntegerBlock.of(0, 1, 2, 3),
                IntegerBlock.of(3, 4, 9, 7)))
            .addChunk(new Chunk(
                IntegerBlock.of(4, 5, 6, 7),
                IntegerBlock.of(5, 3, 8, 10)))
            .build(),
        ImmutableList.of(0),
        ImmutableList.of(1));

    private static final List<DataType> SIMPLE_CASE_INNER_TYPES =
        ImmutableList.of(DataTypes.IntegerType, DataTypes.StringType);
    private static final List<DataType> SIMPLE_CASE_OUTER_TYPES =
        ImmutableList.of(DataTypes.IntegerType, DataTypes.IntegerType);

    public static EquiJoinMockData SIMPLE_CASE = new EquiJoinMockData(
        SIMPLE_CASE_INNER_TYPES,
        new RowChunksBuilder(SIMPLE_CASE_INNER_TYPES).
            addChunk(new Chunk(
                IntegerBlock.of(1, 2, 3, 4),
                StringBlock.of("a", "b", "c", null))).
            addChunk(new Chunk(
                IntegerBlock.of(5, 6, 7, 8),
                StringBlock.of("d", "e", "f", null))).
            row(null, "XX").
            build(),
        SIMPLE_CASE_OUTER_TYPES,
        new RowChunksBuilder(SIMPLE_CASE_OUTER_TYPES).
            addChunk(new Chunk(
                IntegerBlock.of(0, 1, 2, 3),
                IntegerBlock.of(3, 4, 9, 7))).
            addChunk(new Chunk(
                IntegerBlock.of(4, 5, 6, 7),
                IntegerBlock.of(5, 3, 8, 1))).
            row(1000, null)
            .build(),
        ImmutableList.of(0),
        ImmutableList.of(1));

    private static final List<DataType> SINGLE_JOIN_CASE_INNER_TYPES =
        ImmutableList.of(DataTypes.StringType, DataTypes.IntegerType);
    private static final List<DataType> SINGLE_JOIN_CASE_OUTER_TYPES =
        ImmutableList.of(DataTypes.IntegerType, DataTypes.IntegerType);

    public static EquiJoinMockData SINGLE_JOIN_CASE = new EquiJoinMockData(
        SINGLE_JOIN_CASE_INNER_TYPES,
        new RowChunksBuilder(SINGLE_JOIN_CASE_INNER_TYPES).
            addChunk(new Chunk(
                StringBlock.of("a", "b", "c", null),
                IntegerBlock.of(1, 2, 3, 4))).
            addChunk(new Chunk(
                StringBlock.of("d", "e", "f", null),
                IntegerBlock.of(5, 6, 7, 8))).
            row("XX", null).
            build(),
        SINGLE_JOIN_CASE_OUTER_TYPES,
        new RowChunksBuilder(SINGLE_JOIN_CASE_OUTER_TYPES).
            addChunk(new Chunk(
                IntegerBlock.of(0, 1, 2, 3),
                IntegerBlock.of(3, 4, 9, 7)))
            .addChunk(new Chunk(
                IntegerBlock.of(4, 5, 6, 7),
                IntegerBlock.of(5, 3, 8, 1)))
            .row(1000, null)
            .build(),
        ImmutableList.of(1),
        ImmutableList.of(1));

    private static final List<DataType> MULTI_KEY_CASE_INNER_TYPES =
        ImmutableList.of(DataTypes.IntegerType, DataTypes.StringType, DataTypes.StringType);
    private static final List<DataType> MULTI_KEY_CASE_OUTER_TYPES =
        ImmutableList.of(DataTypes.IntegerType, DataTypes.IntegerType, DataTypes.StringType);

    public static EquiJoinMockData MULTI_KEY_CASE = new EquiJoinMockData(
        MULTI_KEY_CASE_INNER_TYPES,
        new RowChunksBuilder(MULTI_KEY_CASE_INNER_TYPES).
            addChunk(new Chunk(
                IntegerBlock.of(1, 2, 3),
                StringBlock.of("a", "a", "a"),
                StringBlock.of("A", "B", "C"))).
            addChunk(new Chunk(
                IntegerBlock.of(1, 2, 3, null),
                StringBlock.of("a", "b", "c", "b"),
                StringBlock.of("E", "F", "G", "H"))).
            row(null, null, "NN").
            row(null, "XX", "YN").
            row(1000, null, "NY").
            row(1000, "XX", "YY").
            build(),
        MULTI_KEY_CASE_OUTER_TYPES,
        new RowChunksBuilder(MULTI_KEY_CASE_OUTER_TYPES).
            addChunk(new Chunk(
                IntegerBlock.of(0, 1, 2, 3, 4),
                IntegerBlock.of(1, 1, 2, 2, null),
                StringBlock.of("a", "b", "a", "b", "a"))).
            addChunk(new Chunk(
                IntegerBlock.of(5, 6, 7, 8, 9),
                IntegerBlock.of(3, 3, 4, 4, 4),
                StringBlock.of("a", "b", "a", "b", null))).
            row(8008, null, null).
            row(8108, null, "XX").
            row(8018, 1000, null).
            row(8118, 1000, "XX").
            build(),
        ImmutableList.of(0, 1),
        ImmutableList.of(1, 2));

    final List<DataType> innerTypes;
    final List<DataType> outerTypes;
    final List<Chunk> innerChunks;
    final List<Chunk> outerChunks;
    final List<Integer> innerKeyIndexes;
    final List<Integer> outerKeyIndexes;
    boolean[] keyIsNullSafe;

    public EquiJoinMockData(List<DataType> innerTypes, List<Chunk> innerChunks,
                            List<DataType> outerTypes, List<Chunk> outerChunks,
                            List<Integer> innerKeyIndexes, List<Integer> outerKeyIndexes) {
        this.innerTypes = innerTypes;
        this.innerChunks = innerChunks;
        this.outerTypes = outerTypes;
        this.outerChunks = outerChunks;
        assert innerKeyIndexes.size() == outerKeyIndexes.size();
        this.innerKeyIndexes = innerKeyIndexes;
        this.outerKeyIndexes = outerKeyIndexes;
        keyIsNullSafe = new boolean[innerChunks.size()];
    }

    void setKeyIsNullSafe(int i, boolean val) {
        keyIsNullSafe[i] = val;
    }

    public void setKeyIsNullSafe(int i) {
        setKeyIsNullSafe(i, true);
    }

    public void setKeyIsNullSafeWithMask(int mask) {
        final int numKeys = innerKeyIndexes.size();
        for (int i = 0; i < numKeys; i++) {
            if (((1 << i) & mask) != 0) {
                setKeyIsNullSafe(i);
            }
        }
    }

    public List<Chunk> getInnerChunks() {
        return innerChunks;
    }

    public List<Chunk> getOuterChunks() {
        return outerChunks;
    }

    public List<DataType> getInnerTypes() {
        return innerTypes;
    }

    public List<DataType> getOuterTypes() {
        return outerTypes;
    }

    public List<EquiJoinKey> getEquiJoinKeysAndReset() {
        final int numKeys = innerKeyIndexes.size();
        List<EquiJoinKey> results = new ArrayList<>(numKeys);
        for (int i = 0; i < numKeys; i++) {
            int outerIndex = outerKeyIndexes.get(i);
            int innerIndex = innerKeyIndexes.get(i);

            DataType outerType = outerTypes.get(outerIndex);
            DataType innerType = innerTypes.get(innerIndex);
            // guaranteed by test case
            DataType unifiedType = outerType;

            results.add(new EquiJoinKey(outerIndex, innerIndex, unifiedType, keyIsNullSafe[i]));

            keyIsNullSafe[i] = false;
        }

        return results;
    }
}