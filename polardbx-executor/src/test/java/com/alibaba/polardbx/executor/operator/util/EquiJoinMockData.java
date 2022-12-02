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

    private static List<DataType> SIMPLE_CASE_INNER_TYPES =
        ImmutableList.of(DataTypes.IntegerType, DataTypes.StringType);
    private static List<DataType> SIMPLE_CASE_OUTER_TYPES =
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

    private static List<DataType> SINGLE_JOIN_CASE_INNER_TYPES =
        ImmutableList.of(DataTypes.StringType, DataTypes.IntegerType);
    private static List<DataType> SINGLE_JOIN_CASE_OUTER_TYPES =
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

    private static List<DataType> MULTI_KEY_CASE_INNER_TYPES =
        ImmutableList.of(DataTypes.IntegerType, DataTypes.StringType, DataTypes.StringType);
    private static List<DataType> MULTI_KEY_CASE_OUTER_TYPES =
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

    private static List<DataType> SEMI_CASE_INNER_TYPES =
        ImmutableList.of(DataTypes.IntegerType);
    private static List<DataType> SEMI_CASE_OUTER_TYPES =
        ImmutableList.of(DataTypes.IntegerType, DataTypes.IntegerType);

    public static EquiJoinMockData SEMI_CASE = new EquiJoinMockData(
        SEMI_CASE_INNER_TYPES,
        new RowChunksBuilder(SEMI_CASE_INNER_TYPES)
            .addChunk(new Chunk(
                IntegerBlock.of(1, 2, 3, 4)))
            .addChunk(new Chunk(
                IntegerBlock.of(3, 4, 5, 6)))
            .build(),
        SEMI_CASE_OUTER_TYPES,
        new RowChunksBuilder(SEMI_CASE_OUTER_TYPES)
            .addChunk(new Chunk(
                IntegerBlock.of(0, 1, 2, 3),
                IntegerBlock.of(3, 4, 9, 7)))
            .addChunk(new Chunk(
                IntegerBlock.of(4, 5, 6, 7),
                IntegerBlock.of(5, 3, 8, null)))
            .build(),
        ImmutableList.of(0),
        ImmutableList.of(1));

    final List<DataType> innerTypes;
    final List<DataType> outerTypes;
    final List<Chunk> innerChunks;
    final List<Chunk> outerChunks;
    final List<Integer> innerKeyIndexes;
    final List<Integer> outerKeyIndexes;
    boolean keyIsNullSafe[];

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
            assert innerType.equals(outerType);
            results.add(new EquiJoinKey(outerIndex, innerIndex, innerType, keyIsNullSafe[i]));

            keyIsNullSafe[i] = false;
        }

        return results;
    }
}