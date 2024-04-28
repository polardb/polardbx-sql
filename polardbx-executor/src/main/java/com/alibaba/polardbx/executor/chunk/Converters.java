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

package com.alibaba.polardbx.executor.chunk;

import com.alibaba.polardbx.common.charset.CollationName;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.SliceType;
import com.alibaba.polardbx.optimizer.utils.DataTypeUtils;

import java.util.List;

/**
 * Chunk and Block Converters
 *
 * @author Eric Fu
 */
public abstract class Converters {

    public static ChunkConverter createChunkConverter(
        List<DataType> inputColumns, int[] columnIndexes, DataType[] targetTypes,
        ExecutionContext context) {
        assert columnIndexes.length == targetTypes.length;

        BlockConverter[] blockConverters = new BlockConverter[columnIndexes.length];
        for (int i = 0; i < columnIndexes.length; i++) {
            blockConverters[i] =
                createBlockConverter(inputColumns.get(columnIndexes[i]), targetTypes[i], context);
        }
        return new ChunkConverter(blockConverters, columnIndexes);
    }

    public static ChunkConverter createChunkConverter(int[] columnIndexes, DataType[] sourceTypes,
                                                      DataType[] targetTypes, ExecutionContext context) {
        BlockConverter[] blockConverters = new BlockConverter[targetTypes.length];
        for (int i = 0; i < targetTypes.length; i++) {
            blockConverters[i] = createBlockConverter(sourceTypes[columnIndexes[i]], targetTypes[i], context);
        }
        return new ChunkConverter(blockConverters, columnIndexes.clone());
    }

    public static ChunkConverter createChunkConverter(int[] columnIndexes, DataType[] sourceTypes,
                                                      DataType[] targetTypes, int offset, ExecutionContext context) {
        BlockConverter[] blockConverters = new BlockConverter[targetTypes.length];
        for (int i = 0; i < targetTypes.length; i++) {
            blockConverters[i] = createBlockConverter(sourceTypes[columnIndexes[i]], targetTypes[i], context);
        }
        int[] tempColumnIndexes = columnIndexes;
        if (offset > 0) {
            tempColumnIndexes = new int[blockConverters.length];
            for (int i = 0; i < blockConverters.length; i++) {
                tempColumnIndexes[i] = columnIndexes[i] - offset;
            }
        }
        return new ChunkConverter(blockConverters, tempColumnIndexes);
    }

    public static ChunkConverter createChunkConverter(
        int[] columnIndexes,
        List<DataType> sourceTypes,
        List<DataType> targetTypes,
        ExecutionContext context) {
        BlockConverter[] blockConverters = new BlockConverter[targetTypes.size()];
        for (int i = 0; i < targetTypes.size(); i++) {
            blockConverters[i] = createBlockConverter(sourceTypes.get(columnIndexes[i]), targetTypes.get(i), context);
        }
        return new ChunkConverter(blockConverters, columnIndexes);
    }

    public static ChunkConverter createChunkConverter(List<DataType> sourceTypes, List<DataType> targetTypes,
                                                      ExecutionContext context) {
        assert sourceTypes.size() == targetTypes.size();
        int[] columnIndexes = new int[sourceTypes.size()];
        for (int i = 0; i < sourceTypes.size(); i++) {
            columnIndexes[i] = i;
        }
        return createChunkConverter(columnIndexes, sourceTypes, targetTypes, context);
    }

    public static BlockConverter createBlockConverter(DataType sourceType, DataType targetType,
                                                      ExecutionContext context) {
        if (context.isEnableOrcRawTypeBlock()) {
            // Raw type do not need to convert.
            return BlockConverter.IDENTITY;
        }
        if (DataTypeUtil.equalsSemantically(sourceType, targetType)) {
            if (sourceType instanceof SliceType
                && targetType instanceof SliceType
                && sourceType.getCollationName() != targetType.getCollationName()) {
                // handle mix of collation.
                CollationName targetCollation = targetType.getCollationName();
                return new CollationConverter(targetCollation);
            }
            return BlockConverter.IDENTITY;
        } else {
            return new BlockTypeConverter(targetType, context);
        }
    }

    private static class BlockTypeConverter implements BlockConverter {

        private final DataType targetType;
        private final ExecutionContext context;

        /**
         * if targetType is Enum , make sure the enumValues is not empty.
         */
        BlockTypeConverter(DataType targetType, ExecutionContext context) {
            this.targetType = targetType;
            this.context = context;
        }

        @Override
        public Block apply(Block block) {
            BlockBuilder blockBuilder = BlockBuilders.create(targetType, context, block.getPositionCount());
            for (int i = 0; i < block.getPositionCount(); i++) {
                //enum convert value from enumValues
                Object converted = DataTypeUtils.convert(targetType, block.getObject(i));
                blockBuilder.writeObject(converted);
            }
            return blockBuilder.build();
        }
    }

    private static class CollationConverter implements BlockConverter {

        private final CollationName collationName;

        CollationConverter(CollationName collationName) {
            this.collationName = collationName;
        }

        @Override
        public Block apply(Block block) {
            if (block instanceof SliceBlock) {
                block.cast(SliceBlock.class).resetCollation(collationName);
            }
            return block;
        }
    }

}
