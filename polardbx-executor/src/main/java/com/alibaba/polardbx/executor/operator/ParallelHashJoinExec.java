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

package com.alibaba.polardbx.executor.operator;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.ChunkConverter;
import com.alibaba.polardbx.executor.chunk.IntegerBlock;
import com.alibaba.polardbx.executor.operator.util.ChunksIndex;
import com.alibaba.polardbx.executor.mpp.execution.TaskExecutor;
import com.alibaba.polardbx.executor.operator.util.AntiJoinResultIterator;
import com.alibaba.polardbx.executor.operator.util.TypedList;
import com.alibaba.polardbx.executor.operator.util.TypedListHandle;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.IntegerType;
import com.alibaba.polardbx.optimizer.core.datatype.LongType;
import com.alibaba.polardbx.optimizer.core.expression.calc.IExpression;
import com.alibaba.polardbx.optimizer.core.expression.calc.InputRefExpression;
import com.alibaba.polardbx.optimizer.core.expression.calc.ScalarFunctionExpression;
import com.alibaba.polardbx.optimizer.core.function.calc.scalar.filter.NotEqual;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;
import com.alibaba.polardbx.optimizer.core.join.EquiJoinKey;
import com.alibaba.polardbx.optimizer.core.row.JoinRow;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.memory.MemoryPoolUtils;
import org.apache.calcite.rel.core.JoinRelType;

import java.text.MessageFormat;
import java.util.List;
import java.util.concurrent.locks.StampedLock;

import static com.alibaba.polardbx.executor.utils.ExecUtils.buildOneChunk;

/**
 * Parallel Hash-Join Executor
 *
 */
public class ParallelHashJoinExec extends AbstractHashJoinExec {
    private static final Logger LOGGER = LoggerFactory.getLogger(TaskExecutor.class);

    private final int probeParallelism;
    private final JoinKeyType joinKeyType;
    private Synchronizer shared;
    private boolean finished;
    private ListenableFuture<?> blocked;
    private int buildChunkSize = 0;
    private int operatorIndex = -1;
    private boolean probeInputIsFinish = false;

    public ParallelHashJoinExec(Synchronizer synchronizer,
                                Executor outerInput,
                                Executor innerInput,
                                JoinRelType joinType,
                                boolean maxOneRow,
                                List<EquiJoinKey> joinKeyTuples,
                                IExpression otherCondition,
                                List<IExpression> antiJoinOperands,
                                boolean buildOuterInput,
                                ExecutionContext context,
                                int operatorIndex,
                                int probeParallelism,
                                boolean keepPartition) {
        super(outerInput, innerInput, joinType, maxOneRow, joinKeyTuples, otherCondition, antiJoinOperands, context);
        super.keepPartition = keepPartition;
        this.shared = Preconditions.checkNotNull(synchronizer);
        this.finished = false;
        this.blocked = ProducerExecutor.NOT_BLOCKED;

        this.buildOuterInput = buildOuterInput;
        this.operatorIndex = operatorIndex;
        this.probeParallelism = probeParallelism;
        if (buildOuterInput) {
            if (super.semiJoin) {
                // do nothing
            } else {
                this.shared.recordOperatorIds(operatorIndex);
            }
        }
        this.joinKeyType = getJoinKeyType(joinKeys);

        boolean enableVecBuildJoinRow =
            context.getParamManager().getBoolean(ConnectionParams.ENABLE_VEC_BUILD_JOIN_ROW);

        boolean isNotNullSafeJoin = joinKeys.stream().noneMatch(EquiJoinKey::isNullSafeEqual);
        boolean isSimpleInnerJoin =
            joinType == JoinRelType.INNER && condition == null && !semiJoin && isNotNullSafeJoin;

        if (isSimpleInnerJoin) {
            buildSimpleInnerProbe(enableVecBuildJoinRow);
        } else if (joinType == JoinRelType.SEMI && buildOuterInput) {
            buildReverseSemiProbe(synchronizer, isNotNullSafeJoin, enableVecBuildJoinRow);
        } else if (joinType == JoinRelType.ANTI && buildOuterInput) {
            buildReverseAntiProbe(synchronizer, isNotNullSafeJoin, enableVecBuildJoinRow);
        } else if (joinType == JoinRelType.SEMI || joinType == JoinRelType.ANTI) {
            buildVecSemiAntiProbe(isNotNullSafeJoin, enableVecBuildJoinRow);
        } else {
            buildDefaultProbe();
        }
    }

    private void buildDefaultProbe() {
        this.probeOperator = new DefaultProbeOperator(true);
    }

    private JoinKeyType getJoinKeyType(List<EquiJoinKey> joinKeys) {
        if (joinKeys.size() == 1) {
            EquiJoinKey equiJoinKey = joinKeys.get(0);
            DataType outerType = outerInput.getDataTypes().get(equiJoinKey.getOuterIndex());
            DataType innerType = innerInput.getDataTypes().get(equiJoinKey.getInnerIndex());
            if (!innerType.equalDeeply(outerType)) {
                return JoinKeyType.OTHER;
            }
            boolean isSingleLongType = equiJoinKey.getUnifiedType() instanceof LongType;
            if (isSingleLongType) {
                return JoinKeyType.LONG;
            }
            boolean isSingleIntegerType = equiJoinKey.getUnifiedType() instanceof IntegerType;
            if (isSingleIntegerType) {
                return JoinKeyType.INTEGER;
            }
            return JoinKeyType.OTHER;
        }

        boolean isMultiIntegerType =
            joinKeys.stream().allMatch(key -> {
                DataType outerType = outerInput.getDataTypes().get(key.getOuterIndex());
                DataType innerType = innerInput.getDataTypes().get(key.getInnerIndex());
                boolean isSame = innerType.equalDeeply(outerType);
                boolean isInteger = key.getUnifiedType() instanceof IntegerType;
                return isInteger && isSame;
            });
        if (isMultiIntegerType) {
            return JoinKeyType.MULTI_INTEGER;
        }
        return JoinKeyType.OTHER;
    }

    private void buildSimpleInnerProbe(boolean enableVecBuildJoinRow) {
        if (!useVecJoin) {
            buildDefaultProbe();
            return;
        }
        switch (joinKeyType) {
        case LONG:
            buildSingleLongProbe(enableVecBuildJoinRow);
            break;
        case INTEGER:
            buildSingleIntProbe(enableVecBuildJoinRow);
            break;
        case MULTI_INTEGER:
            buildMultiIntProbe(enableVecBuildJoinRow);
            break;
        case OTHER:
            buildDefaultProbe();
            break;
        default:
            throw new UnsupportedOperationException(
                "Unsupported joinKeyType: " + joinKeyType + ", should not reach here");
        }
    }

    private void buildSingleLongProbe(boolean enableVecBuildJoinRow) {
        this.probeOperator = new LongProbeOperator(enableVecBuildJoinRow);
        shared.builderKeyChunks.setTypedHashTable(new TypedListHandle() {
            private TypedList[] typedLists;

            @Override
            public long estimatedSize(int fixedSize) {
                return TypedList.IntTypedList.estimatedSizeInBytes(fixedSize);
            }

            @Override
            public TypedList[] getTypedLists(int fixedSize) {
                if (typedLists == null) {
                    typedLists = new TypedList[] {TypedList.createLong(fixedSize)};
                }
                return typedLists;
            }

            @Override
            public void consume(Chunk chunk, int sourceIndex) {
                chunk.getBlock(0).cast(Block.class)
                    .appendTypedHashTable(typedLists[0], sourceIndex, 0, chunk.getPositionCount());
            }
        });
    }

    private void buildSingleIntProbe(boolean enableVecBuildJoinRow) {
        this.probeOperator = new IntProbeOperator(enableVecBuildJoinRow);
        shared.builderKeyChunks.setTypedHashTable(new TypedListHandle() {
            private TypedList[] typedLists;

            @Override
            public long estimatedSize(int fixedSize) {
                return TypedList.IntTypedList.estimatedSizeInBytes(fixedSize);
            }

            @Override
            public TypedList[] getTypedLists(int fixedSize) {
                if (typedLists == null) {
                    typedLists = new TypedList[] {TypedList.createInt(fixedSize)};
                }
                return typedLists;
            }

            @Override
            public void consume(Chunk chunk, int sourceIndex) {
                chunk.getBlock(0).cast(Block.class)
                    .appendTypedHashTable(typedLists[0], sourceIndex, 0, chunk.getPositionCount());
            }
        });
    }

    private void buildMultiIntProbe(boolean enableVecBuildJoinRow) {
        this.probeOperator = new MultiIntProbeOperator(joinKeys.size(), enableVecBuildJoinRow);
        shared.builderKeyChunks.setTypedHashTable(new TypedListHandle() {
            private TypedList[] typedLists;

            @Override
            public long estimatedSize(int fixedSize) {
                final int targetSize = fixedSize * ((getBuildKeyChunkGetter().columnWidth() + 1) / 2);
                return TypedList.LongTypedList.estimatedSizeInBytes(targetSize);
            }

            @Override
            public TypedList[] getTypedLists(int fixedSize) {
                // The width of comparison serialized number is (blockCount + 1) / 2
                if (typedLists == null) {
                    final int targetSize = fixedSize * ((getBuildKeyChunkGetter().columnWidth() + 1) / 2);
                    typedLists = new TypedList[] {TypedList.createLong(targetSize)};
                }
                return typedLists;
            }

            @Override
            public void consume(Chunk chunk, int sourceIndex) {
                final int blockCount = chunk.getBlockCount();
                final int positionCount = chunk.getPositionCount();
                int[][] arrays = new int[blockCount][0];
                for (int blockIndex = 0; blockIndex < blockCount; blockIndex++) {
                    arrays[blockIndex] = chunk.getBlock(blockIndex).cast(IntegerBlock.class).intArray();
                }

                // The width of comparison serialized number is (blockCount + 1) / 2
                if (blockCount % 2 == 0) {
                    // when block count is even number.
                    for (int i = 0; i < positionCount; i++) {
                        for (int blockIndex = 0; blockIndex < blockCount; blockIndex += 2) {
                            long serialized =
                                TypedListHandle.serialize(arrays[blockIndex][i], arrays[blockIndex + 1][i]);
                            typedLists[0].setLong(sourceIndex++, serialized);
                        }
                    }
                } else {
                    // when block count is odd number.
                    for (int i = 0; i < positionCount; i++) {
                        for (int blockIndex = 0; blockIndex < blockCount - 1; blockIndex += 2) {
                            long serialized =
                                TypedListHandle.serialize(arrays[blockIndex][i], arrays[blockIndex + 1][i]);
                            typedLists[0].setLong(sourceIndex++, serialized);
                        }
                        long serialized = TypedListHandle.serialize(arrays[blockCount - 1][i], 0);
                        typedLists[0].setLong(sourceIndex++, serialized);
                    }
                }

            }
        });
    }

    private void buildReverseSemiProbe(Synchronizer synchronizer, boolean isNotNullSafeJoin,
                                       boolean enableVecBuildJoinRow) {
        // try type-specific implementation first
        if (useVecJoin && isNotNullSafeJoin && condition == null) {
            switch (joinKeyType) {
            case LONG:
                buildReverseSemiLongProbe(synchronizer, enableVecBuildJoinRow);
                return;
            case INTEGER:
                buildReverseSemiIntProbe(synchronizer, enableVecBuildJoinRow);
                return;
            default:
                // fall through
            }
        }

        // try matched condition cases
        boolean isSemiLongNotEq = joinKeyType == JoinKeyType.LONG;
        boolean isSemiIntegerNotEq = joinKeyType == JoinKeyType.INTEGER;
        int buildChunkConditionIndex = -1;
        if (!useAntiCondition && isScalarInputRefCondition()) {
            boolean isNotEq = ((ScalarFunctionExpression) condition).isA(NotEqual.class);

            isSemiLongNotEq &= isNotEq;
            isSemiIntegerNotEq &= isNotEq;
            // since this is outer build (reverse)
            buildChunkConditionIndex = getBuildChunkConditionIndex();
        } else {
            isSemiLongNotEq = false;
            isSemiIntegerNotEq = false;
        }

        if (isSemiLongNotEq || isSemiIntegerNotEq) {
            JoinKeyType conditionType = getConditionKeyType((ScalarFunctionExpression) condition);

            if (useVecJoin && isNotNullSafeJoin && isSemiLongNotEq && conditionType == JoinKeyType.INTEGER) {
                buildReverseSemiLongNotEqIntProbe(synchronizer, enableVecBuildJoinRow, buildChunkConditionIndex);
                return;
            } else if (useVecJoin && isNotNullSafeJoin && isSemiIntegerNotEq && conditionType == JoinKeyType.INTEGER) {
                buildReverseSemiIntNotEqIntProbe(synchronizer, enableVecBuildJoinRow, buildChunkConditionIndex);
                return;
            }
        }

        // normal cases
        if (super.condition == null) {
            this.probeOperator = new SimpleReverseSemiProbeOperator(synchronizer);
        } else {
            this.probeOperator = new ReverseSemiProbeOperator(synchronizer);
        }
    }

    private void buildReverseSemiLongProbe(Synchronizer synchronizer, boolean enableVecBuildJoinRow) {
        this.probeOperator = new ReverseSemiLongProbeOperator(synchronizer);
        shared.builderKeyChunks.setTypedHashTable(new TypedListHandle() {
            private TypedList[] typedLists;

            @Override
            public long estimatedSize(int fixedSize) {
                return TypedList.LongTypedList.estimatedSizeInBytes(fixedSize);
            }

            @Override
            public TypedList[] getTypedLists(int fixedSize) {
                if (typedLists == null) {
                    typedLists = new TypedList[] {TypedList.createLong(fixedSize)};
                }
                return typedLists;
            }

            @Override
            public void consume(Chunk chunk, int sourceIndex) {
                chunk.getBlock(0).appendTypedHashTable(typedLists[0], sourceIndex, 0, chunk.getPositionCount());
            }
        });

    }

    private void buildReverseSemiIntProbe(Synchronizer synchronizer, boolean enableVecBuildJoinRow) {
        this.probeOperator = new ReverseSemiIntProbeOperator(synchronizer);
        shared.builderKeyChunks.setTypedHashTable(new TypedListHandle() {
            private TypedList[] typedLists;

            @Override
            public long estimatedSize(int fixedSize) {
                return TypedList.IntTypedList.estimatedSizeInBytes(fixedSize);
            }

            @Override
            public TypedList[] getTypedLists(int fixedSize) {
                if (typedLists == null) {
                    typedLists = new TypedList[] {TypedList.createInt(fixedSize)};
                }
                return typedLists;
            }

            @Override
            public void consume(Chunk chunk, int sourceIndex) {
                chunk.getBlock(0).appendTypedHashTable(typedLists[0], sourceIndex, 0, chunk.getPositionCount());
            }
        });
    }

    private void buildReverseSemiLongNotEqIntProbe(Synchronizer synchronizer, boolean enableVecBuildJoinRow,
                                                   int buildChunkConditionIndex) {
        this.probeOperator = new ReverseSemiLongNotEqIntegerProbeOperator(synchronizer);
        shared.builderChunks.setTypedHashTable(new TypedListHandle() {
            private TypedList[] typedLists;

            @Override
            public long estimatedSize(int fixedSize) {
                return TypedList.IntTypedList.estimatedSizeInBytes(fixedSize);
            }

            @Override
            public TypedList[] getTypedLists(int fixedSize) {
                if (typedLists == null) {
                    typedLists = new TypedList[] {TypedList.createInt(fixedSize)};
                }
                return typedLists;
            }

            @Override
            public void consume(Chunk chunk, int sourceIndex) {
                chunk.getBlock(buildChunkConditionIndex)
                    .appendTypedHashTable(typedLists[0], sourceIndex, 0, chunk.getPositionCount());
            }
        });
        shared.builderKeyChunks.setTypedHashTable(new TypedListHandle() {
            private TypedList[] typedLists;

            @Override
            public long estimatedSize(int fixedSize) {
                return TypedList.LongTypedList.estimatedSizeInBytes(fixedSize);
            }

            @Override
            public TypedList[] getTypedLists(int fixedSize) {
                if (typedLists == null) {
                    typedLists = new TypedList[] {TypedList.createLong(fixedSize)};
                }
                return typedLists;
            }

            @Override
            public void consume(Chunk chunk, int sourceIndex) {
                chunk.getBlock(0).appendTypedHashTable(typedLists[0], sourceIndex, 0, chunk.getPositionCount());
            }
        });
    }

    private void buildReverseSemiIntNotEqIntProbe(Synchronizer synchronizer, boolean enableVecBuildJoinRow,
                                                  int buildChunkConditionIndex) {
        this.probeOperator = new ReverseSemiIntNotEqIntegerProbeOperator(synchronizer);
        shared.builderChunks.setTypedHashTable(new TypedListHandle() {
            private TypedList[] typedLists;

            @Override
            public long estimatedSize(int fixedSize) {
                return TypedList.IntTypedList.estimatedSizeInBytes(fixedSize);
            }

            @Override
            public TypedList[] getTypedLists(int fixedSize) {
                if (typedLists == null) {
                    typedLists = new TypedList[] {TypedList.createInt(fixedSize)};
                }
                return typedLists;
            }

            @Override
            public void consume(Chunk chunk, int sourceIndex) {
                chunk.getBlock(buildChunkConditionIndex)
                    .appendTypedHashTable(typedLists[0], sourceIndex, 0, chunk.getPositionCount());
            }
        });
        shared.builderKeyChunks.setTypedHashTable(new TypedListHandle() {
            private TypedList[] typedLists;

            @Override
            public long estimatedSize(int fixedSize) {
                return TypedList.IntTypedList.estimatedSizeInBytes(fixedSize);
            }

            @Override
            public TypedList[] getTypedLists(int fixedSize) {
                if (typedLists == null) {
                    typedLists = new TypedList[] {TypedList.createInt(fixedSize)};
                }
                return typedLists;
            }

            @Override
            public void consume(Chunk chunk, int sourceIndex) {
                chunk.getBlock(0).appendTypedHashTable(typedLists[0], sourceIndex, 0, chunk.getPositionCount());
            }
        });
    }

    private void buildReverseAntiProbe(Synchronizer synchronizer, boolean isNotNullSafeJoin,
                                       boolean enableVecBuildJoinRow) {
        // try type-specific implementation first
        boolean isAntiLongNotEqInteger = joinKeyType == JoinKeyType.LONG;
        boolean isAntiIntegerNotEqInteger = joinKeyType == JoinKeyType.INTEGER;
        int buildChunkConditionIndex = -1;
        if (!useAntiCondition && isScalarInputRefCondition()) {
            boolean isNotEq = ((ScalarFunctionExpression) condition).isA(NotEqual.class);
            isAntiLongNotEqInteger &= isNotEq;
            isAntiIntegerNotEqInteger &= isNotEq;
            buildChunkConditionIndex = getBuildChunkConditionIndex();
        } else {
            isAntiLongNotEqInteger = false;
            isAntiIntegerNotEqInteger = false;
        }

        if (useVecJoin && isNotNullSafeJoin && isAntiLongNotEqInteger) {
            buildReverseAntiLongNotEqIntProbe(synchronizer, enableVecBuildJoinRow, buildChunkConditionIndex);
            return;
        } else if (useVecJoin && isNotNullSafeJoin && isAntiIntegerNotEqInteger) {
            buildReverseAntiIntNotEqIntProbe(synchronizer, enableVecBuildJoinRow, buildChunkConditionIndex);
            return;
        }

        if (useVecJoin && isNotNullSafeJoin && joinKeyType == JoinKeyType.INTEGER
            && condition == null && !useAntiCondition) {

            buildReversAntiIntProbe(synchronizer, enableVecBuildJoinRow);
            return;
        }

        // normal cases
        if (this.antiJoinOperands == null && this.antiCondition == null && super.condition == null) {
            this.probeOperator = new SimpleReverseAntiProbeOperator(synchronizer);
        } else if (this.antiJoinOperands == null && this.antiCondition == null) {
            // with join condition
            this.probeOperator = new ReverseAntiProbeOperator(synchronizer);
        } else {
            // should not access here
            throw new RuntimeException(String.format("reverse anti hash join not support this, "
                + "antiJoinOperands is %s, antiCondition is %s", this.antiJoinOperands, this.antiCondition));
        }
    }

    private void buildReverseAntiLongNotEqIntProbe(Synchronizer synchronizer, boolean enableVecBuildJoinRow,
                                                   int buildChunkConditionIndex) {
        this.probeOperator = new ReverseAntiLongNotEqIntegerProbeOperator(synchronizer);
        shared.builderChunks.setTypedHashTable(new TypedListHandle() {
            private TypedList[] typedLists;

            @Override
            public long estimatedSize(int fixedSize) {
                return TypedList.IntTypedList.estimatedSizeInBytes(fixedSize);
            }

            @Override
            public TypedList[] getTypedLists(int fixedSize) {
                if (typedLists == null) {
                    typedLists = new TypedList[] {TypedList.createInt(fixedSize)};
                }
                return typedLists;
            }

            @Override
            public void consume(Chunk chunk, int sourceIndex) {
                chunk.getBlock(buildChunkConditionIndex)
                    .appendTypedHashTable(typedLists[0], sourceIndex, 0, chunk.getPositionCount());
            }
        });
        shared.builderKeyChunks.setTypedHashTable(new TypedListHandle() {
            private TypedList[] typedLists;

            @Override
            public long estimatedSize(int fixedSize) {
                return TypedList.LongTypedList.estimatedSizeInBytes(fixedSize);
            }

            @Override
            public TypedList[] getTypedLists(int fixedSize) {
                if (typedLists == null) {
                    typedLists = new TypedList[] {TypedList.createLong(fixedSize)};
                }
                return typedLists;
            }

            @Override
            public void consume(Chunk chunk, int sourceIndex) {
                chunk.getBlock(0).appendTypedHashTable(typedLists[0], sourceIndex, 0, chunk.getPositionCount());
            }
        });
    }

    private void buildReverseAntiIntNotEqIntProbe(Synchronizer synchronizer, boolean enableVecBuildJoinRow,
                                                  int buildChunkConditionIndex) {
        this.probeOperator = new ReverseAntiIntNotEqIntegerProbeOperator(synchronizer);
        shared.builderChunks.setTypedHashTable(new TypedListHandle() {
            private TypedList[] typedLists;

            @Override
            public long estimatedSize(int fixedSize) {
                return TypedList.IntTypedList.estimatedSizeInBytes(fixedSize);
            }

            @Override
            public TypedList[] getTypedLists(int fixedSize) {
                if (typedLists == null) {
                    typedLists = new TypedList[] {TypedList.createInt(fixedSize)};
                }
                return typedLists;
            }

            @Override
            public void consume(Chunk chunk, int sourceIndex) {
                chunk.getBlock(buildChunkConditionIndex)
                    .appendTypedHashTable(typedLists[0], sourceIndex, 0, chunk.getPositionCount());
            }
        });
        shared.builderKeyChunks.setTypedHashTable(new TypedListHandle() {
            private TypedList[] typedLists;

            @Override
            public long estimatedSize(int fixedSize) {
                return TypedList.IntTypedList.estimatedSizeInBytes(fixedSize);
            }

            @Override
            public TypedList[] getTypedLists(int fixedSize) {
                if (typedLists == null) {
                    typedLists = new TypedList[] {TypedList.createInt(fixedSize)};
                }
                return typedLists;
            }

            @Override
            public void consume(Chunk chunk, int sourceIndex) {
                chunk.getBlock(0).appendTypedHashTable(typedLists[0], sourceIndex, 0, chunk.getPositionCount());
            }
        });
    }

    private void buildReversAntiIntProbe(Synchronizer synchronizer, boolean enableVecBuildJoinRow) {
        this.probeOperator = new ReverseAntiIntegerProbeOperator(synchronizer);
        shared.builderKeyChunks.setTypedHashTable(new TypedListHandle() {
            private TypedList[] typedLists;

            @Override
            public long estimatedSize(int fixedSize) {
                return TypedList.IntTypedList.estimatedSizeInBytes(fixedSize);
            }

            @Override
            public TypedList[] getTypedLists(int fixedSize) {
                if (typedLists == null) {
                    typedLists = new TypedList[] {TypedList.createInt(fixedSize)};
                }
                return typedLists;
            }

            @Override
            public void consume(Chunk chunk, int sourceIndex) {
                chunk.getBlock(0).appendTypedHashTable(typedLists[0], sourceIndex, 0, chunk.getPositionCount());
            }
        });
    }

    private void buildVecSemiAntiProbe(boolean isNotNullSafeJoin, boolean enableVecBuildJoinRow) {
        if (!useVecJoin || !isNotNullSafeJoin) {
            buildDefaultProbe();
            return;
        }

        if (joinType == JoinRelType.SEMI && joinKeyType == JoinKeyType.LONG
            && condition == null) {
            buildSemiLongProbe(enableVecBuildJoinRow);
            return;
        }

        // semi join with condition
        int buildChunkConditionIndex = -1;
        boolean isLongKeyNotEq = joinKeyType == JoinKeyType.LONG;
        if (!useAntiCondition && isScalarInputRefCondition()) {
            isLongKeyNotEq &= ((ScalarFunctionExpression) condition).isA(NotEqual.class);
            buildChunkConditionIndex = getBuildChunkConditionIndex();
        } else {
            isLongKeyNotEq = false;
        }

        if (!isLongKeyNotEq || joinType != JoinRelType.SEMI) {
            // to be implemented
            buildDefaultProbe();
            return;
        }

        JoinKeyType conditionKeyType = getConditionKeyType((ScalarFunctionExpression) condition);

        if (conditionKeyType == JoinKeyType.INTEGER) {
            buildSemiLongNotEqIntProbe(enableVecBuildJoinRow, buildChunkConditionIndex);
            return;
        }

        if (conditionKeyType == JoinKeyType.LONG) {
            buildSemiLongNotEqLongProbe(enableVecBuildJoinRow, buildChunkConditionIndex);
            return;
        }

        // normal cases
        buildDefaultProbe();
    }

    protected JoinKeyType getConditionKeyType(ScalarFunctionExpression condition) {
        List<IExpression> args = condition.getArgs();
        Preconditions.checkArgument(args.size() == 2, "Join condition arg count should be 2");

        // get build chunk condition index for TypedHashTable
        int idx1 = ((InputRefExpression) args.get(0)).getInputRefIndex();
        int idx2 = ((InputRefExpression) args.get(1)).getInputRefIndex();
        int minIdx = Math.min(idx1, idx2);
        int maxIdx = Math.max(idx1, idx2);

        DataType type1 = outerInput.getDataTypes().get(minIdx);
        DataType type2 = innerInput.getDataTypes().get(maxIdx - outerInput.getDataTypes().size());
        if (type1 instanceof LongType && type2 instanceof LongType) {
            return JoinKeyType.LONG;
        }
        if (type1 instanceof IntegerType && type2 instanceof IntegerType) {
            return JoinKeyType.INTEGER;
        }
        return JoinKeyType.OTHER;
    }

    private void buildSemiLongProbe(boolean enableVecBuildJoinRow) {
        this.probeOperator = new SemiLongProbeOperator(enableVecBuildJoinRow);
        shared.builderKeyChunks.setTypedHashTable(new TypedListHandle() {
            private TypedList[] typedLists;

            @Override
            public long estimatedSize(int fixedSize) {
                return TypedList.LongTypedList.estimatedSizeInBytes(fixedSize);
            }

            @Override
            public TypedList[] getTypedLists(int fixedSize) {
                if (typedLists == null) {
                    typedLists = new TypedList[] {TypedList.createLong(fixedSize)};
                }
                return typedLists;
            }

            @Override
            public void consume(Chunk chunk, int sourceIndex) {
                chunk.getBlock(0).appendTypedHashTable(typedLists[0], sourceIndex, 0, chunk.getPositionCount());
            }
        });
    }

    private void buildSemiLongNotEqIntProbe(boolean enableVecBuildJoinRow, int buildChunkConditionIndex) {
        this.probeOperator = new SemiLongNotEqIntegerProbeOperator(enableVecBuildJoinRow);
        shared.builderChunks.setTypedHashTable(new TypedListHandle() {
            private TypedList[] typedLists;

            @Override
            public long estimatedSize(int fixedSize) {
                return TypedList.IntTypedList.estimatedSizeInBytes(fixedSize);
            }

            @Override
            public TypedList[] getTypedLists(int fixedSize) {
                if (typedLists == null) {
                    typedLists = new TypedList[] {TypedList.createInt(fixedSize)};
                }
                return typedLists;
            }

            @Override
            public void consume(Chunk chunk, int sourceIndex) {
                chunk.getBlock(buildChunkConditionIndex)
                    .appendTypedHashTable(typedLists[0], sourceIndex, 0, chunk.getPositionCount());
            }
        });
        shared.builderKeyChunks.setTypedHashTable(new TypedListHandle() {
            private TypedList[] typedLists;

            @Override
            public long estimatedSize(int fixedSize) {
                return TypedList.LongTypedList.estimatedSizeInBytes(fixedSize);
            }

            @Override
            public TypedList[] getTypedLists(int fixedSize) {
                if (typedLists == null) {
                    typedLists = new TypedList[] {TypedList.createLong(fixedSize)};
                }
                return typedLists;
            }

            @Override
            public void consume(Chunk chunk, int sourceIndex) {
                chunk.getBlock(0).appendTypedHashTable(typedLists[0], sourceIndex, 0, chunk.getPositionCount());
            }
        });
    }

    private void buildSemiLongNotEqLongProbe(boolean enableVecBuildJoinRow, int buildChunkConditionIndex) {
        this.probeOperator = new SemiLongNotEqLongProbeOperator(enableVecBuildJoinRow);
        shared.builderChunks.setTypedHashTable(new TypedListHandle() {
            private TypedList[] typedLists;

            @Override
            public long estimatedSize(int fixedSize) {
                return TypedList.LongTypedList.estimatedSizeInBytes(fixedSize);
            }

            @Override
            public TypedList[] getTypedLists(int fixedSize) {
                if (typedLists == null) {
                    typedLists = new TypedList[] {TypedList.createLong(fixedSize)};
                }
                return typedLists;
            }

            @Override
            public void consume(Chunk chunk, int sourceIndex) {
                chunk.getBlock(buildChunkConditionIndex)
                    .appendTypedHashTable(typedLists[0], sourceIndex, 0, chunk.getPositionCount());
            }
        });
        shared.builderKeyChunks.setTypedHashTable(new TypedListHandle() {
            private TypedList[] typedLists;

            @Override
            public long estimatedSize(int fixedSize) {
                return TypedList.LongTypedList.estimatedSizeInBytes(fixedSize);
            }

            @Override
            public TypedList[] getTypedLists(int fixedSize) {
                if (typedLists == null) {
                    typedLists = new TypedList[] {TypedList.createLong(fixedSize)};
                }
                return typedLists;
            }

            @Override
            public void consume(Chunk chunk, int sourceIndex) {
                chunk.getBlock(0).appendTypedHashTable(typedLists[0], sourceIndex, 0, chunk.getPositionCount());
            }
        });
    }

    @Override
    public void openConsume() {
        Preconditions.checkArgument(shared != null, "reopen not supported yet");
        // Use one shared memory pool but a local memory allocator
        this.memoryPool = MemoryPoolUtils
            .createOperatorTmpTablePool("ParallelHashJoinExec@" + System.identityHashCode(this),
                context.getMemoryPool());
        this.memoryAllocator = memoryPool.getMemoryAllocatorCtx();
    }

    @Override
    public void doOpen() {
        if (!passThrough && passNothing) {
            //避免初始化probe side, minor optimizer
            return;
        }
        super.doOpen();
    }

    @Override
    public void buildConsume() {
        if (memoryPool != null) {
            long start = System.nanoTime();
            int partition = shared.buildCount.getAndIncrement();
            if (partition < shared.numberOfExec) {
                int[] ignoreNullBlocks = getIgnoreNullsInJoinKey().stream().mapToInt(i -> i).toArray();
                shared.buildHashTable(partition, memoryAllocator, ignoreNullBlocks, ignoreNullBlocks.length);
            }
            long end = System.nanoTime();
            LOGGER.debug(MessageFormat.format("HashJoinExec: {0} build consume time cost = {1} ns, partition is {2}, "
                + "start = {3}, end = {4}", this.toString(), (end - start), partition, start, end));
            // Copy the built hash-table from shared states into this executor
            this.buildChunks = shared.builderChunks;
            this.buildKeyChunks = shared.builderKeyChunks;
            this.hashTable = shared.hashTable;
            this.positionLinks = shared.positionLinks;
            this.bloomFilter = shared.bloomFilter;
            if (buildChunks.isEmpty() && joinType == JoinRelType.INNER) {
                passNothing = true;
            }
            if (semiJoin) {
                doSpecialCheckForSemiJoin();
            }

            this.buildChunkSize = this.buildChunks.getPositionCount();
        }
    }

    @Override
    Chunk doNextChunk() {
        // Special path for pass-through or pass-nothing mode
        if (passThrough) {
            return nextProbeChunk();
        } else if (passNothing) {
            return null;
        }

        if (buildOuterInput && joinType != JoinRelType.SEMI && joinType != JoinRelType.ANTI
            && shared.joinNullRowBitSet == null) {
            shared.buildNullBitSets(buildChunkSize);
        }

        return super.doNextChunk();
    }

    @Override
    Chunk nextProbeChunk() {
        Chunk ret = getProbeInput().nextChunk();
        if (ret == null) {
            probeInputIsFinish = getProbeInput().produceIsFinished();
            blocked = getProbeInput().produceIsBlocked();
        }
        return ret;
    }

    @Override
    public void consumeChunk(Chunk inputChunk) {
        Chunk keyChunk = getBuildKeyChunkGetter().apply(inputChunk);
        shared.nextPartition().appendChunk(inputChunk, keyChunk);
    }

    @Override
    public boolean nextJoinNullRows() {
        synchronized (shared) {
            if (!shared.operatorIds.isEmpty()) {
                return false;
            }
        }
        int matchedPosition = shared.nextUnmatchedPosition();
        if (matchedPosition == -1) {
            return false;
        }
        // first outer side, then inner side
        int col = 0;

        if (joinType != JoinRelType.RIGHT) {
            for (int j = 0; j < outerInput.getDataTypes().size(); j++) {
                buildChunks.writePositionTo(j, matchedPosition, blockBuilders[col++]);
            }
            // single join only output the first row of right side
            final int rightColumns = singleJoin ? 1 : innerInput.getDataTypes().size();
            for (int j = 0; j < rightColumns; j++) {
                blockBuilders[col++].appendNull();
            }
        } else {
            for (int j = 0; j < innerInput.getDataTypes().size(); j++) {
                blockBuilders[col++].appendNull();
            }
            for (int j = 0; j < outerInput.getDataTypes().size(); j++) {
                buildChunks.writePositionTo(j, matchedPosition, blockBuilders[col++]);
            }
        }
        assert col == blockBuilders.length;
        return true;
    }

    @Override
    protected void afterProcess(Chunk outputChunk) {
        super.afterProcess(outputChunk);
        // reverse semi join do not need extra process
        if (buildOuterInput && joinType != JoinRelType.SEMI) {
            if (joinType == JoinRelType.ANTI) {
                // first stage: get all probe chunks and mark the concurrent hash table
                //              outputChunk is always null at this stage, because we return nothing
                //              should not finish, always mark not finish
                //              and should not modify the status of block(meaning maybe blocked under this stage)
                //
                // second stage: report finish info
                //
                // third stage: wait other thread finish probe, should not finish
                //
                // final stage: output result
                //
                // should use outputChunk rather than probeChunk, because we should switch status depend on result
                if (outputChunk == null) {
                    if (probeInputIsFinish) {
                        if (antiJoinResultIterator == null) {
                            int probeNumOfSynchronizer = shared.getProbeParallelism();
                            boolean firstMark = shared.antiProbeFinished.markAndGet(operatorIndex);
                            if (firstMark) {
                                // update anti probe count and build anti join row ids if all probe finished
                                int finishedProbeCount = shared.antiProbeCount.addAndGet(1);
                                if (finishedProbeCount == probeNumOfSynchronizer) {
                                    shared.buildAntiJoinOutputRowIds();
                                }
                            }
                            int finishedProbeCount = shared.antiProbeCount.get();
                            // build result iterator if all probe finished
                            // and anti join output row has been build (important)
                            if (finishedProbeCount == probeNumOfSynchronizer && shared.antJoinOutputRowId != null) {
                                int recordsPerExec = shared.antJoinOutputRowId.size() / probeNumOfSynchronizer;
                                int startOffset = operatorIndex * recordsPerExec;
                                int endOffset = (operatorIndex == probeNumOfSynchronizer - 1)
                                    ? shared.antJoinOutputRowId.size() : (operatorIndex + 1) * recordsPerExec;
                                antiJoinResultIterator =
                                    new AntiJoinResultIterator(shared.antJoinOutputRowId, shared.builderChunks,
                                        blockBuilders,
                                        super.chunkLimit,
                                        startOffset,
                                        endOffset);
                            }
                            finished = false;
                            blocked = ProducerExecutor.NOT_BLOCKED;
                        } else {
                            // anti join result iterator not null, meaning reached final stage
                            if (antiJoinResultIterator.finished()) {
                                finished = true;
                            } else {
                                finished = false;
                            }
                            blocked = ProducerExecutor.NOT_BLOCKED;
                        }
                    } else {
                        // probe input not finish
                        finished = false;
                    }
                } else {
                    finished = false;
                    blocked = ProducerExecutor.NOT_BLOCKED;
                }
            } else {
                if (probeChunk == null) {
                    if (probeInputIsFinish) {
                        if (shared.consumeInputIsFinish(operatorIndex) && shared.finishIterator()) {
                            finished = true;
                            blocked = ProducerExecutor.NOT_BLOCKED;
                        } else {
                            finished = false;
                            blocked = ProducerExecutor.NOT_BLOCKED;
                        }
                    } else {
                        finished = false;
                    }
                } else {
                    finished = false;
                    blocked = ProducerExecutor.NOT_BLOCKED;
                }
            }
        } else {
            if (outputChunk == null) {
                finished = probeInputIsFinish;
            } else {
                finished = false;
                blocked = ProducerExecutor.NOT_BLOCKED;
            }
        }
    }

    @Override
    protected void buildJoinRow(
        ChunksIndex chunksIndex, Chunk probeInputChunk, int position, int matchedPosition) {
        // first outer side, then inner side
        if (buildOuterInput) {
            shared.markUsedKeys(matchedPosition);
            int col = 0;
            for (int i = 0; i < outerInput.getDataTypes().size(); i++) {
                chunksIndex.writePositionTo(i, matchedPosition, blockBuilders[col++]);
            }
            // single join only output the first row of right side
            final int rightColumns = singleJoin ? 1 : innerInput.getDataTypes().size();
            for (int i = 0; i < rightColumns; i++) {
                probeInputChunk.getBlock(i).writePositionTo(position, blockBuilders[col++]);
            }
            assert col == blockBuilders.length;
        } else {
            super.buildJoinRow(chunksIndex, probeInputChunk, position, matchedPosition);
        }
    }

    @Override
    protected void buildRightJoinRow(
        ChunksIndex chunksIndex, Chunk probeInputChunk, int position, int matchedPosition) {
        // first inner side, then outer side
        if (buildOuterInput) {
            shared.markUsedKeys(matchedPosition);
            int col = 0;
            for (int i = 0; i < innerInput.getDataTypes().size(); i++) {
                probeInputChunk.getBlock(i).writePositionTo(position, blockBuilders[col++]);
            }
            for (int i = 0; i < outerInput.getDataTypes().size(); i++) {
                chunksIndex.writePositionTo(i, matchedPosition, blockBuilders[col++]);
            }
            assert col == blockBuilders.length;
        } else {
            super.buildRightJoinRow(chunksIndex, probeInputChunk, position, matchedPosition);
        }
    }

    @Override
    void doClose() {
        // Release the reference to shared hash table etc.
        if (shared != null) {
            if (!(buildOuterInput && semiJoin)) {
                this.shared.consumeInputIsFinish(operatorIndex);
            }
            this.shared = null;
        }
        super.doClose();
    }

    @Override
    public boolean produceIsFinished() {
        return finished || passNothing;
    }

    @Override
    public ListenableFuture<?> produceIsBlocked() {
        return blocked;
    }

    @Override
    public Executor getBuildInput() {
        if (buildOuterInput) {
            return outerInput;
        } else {
            return innerInput;
        }
    }

    @Override
    public Executor getProbeInput() {
        if (buildOuterInput) {
            return innerInput;
        } else {
            return outerInput;
        }
    }

    @Override
    ChunkConverter getBuildKeyChunkGetter() {
        if (buildOuterInput) {
            return outerKeyChunkGetter;
        } else {
            return innerKeyChunkGetter;
        }
    }

    @Override
    ChunkConverter getProbeKeyChunkGetter() {
        if (buildOuterInput) {
            return innerKeyChunkGetter;
        } else {
            return outerKeyChunkGetter;
        }
    }

    @Override
    protected boolean checkJoinCondition(
        ChunksIndex chunksIndex, Chunk outerChunk, int outerPosition, int innerPosition) {
        if (condition == null) {
            return true;
        }

        final Row outerRow = outerChunk.rowAt(outerPosition);
        final Row innerRow = buildChunks.rowAt(innerPosition);

        Row leftRow = joinType.leftSide(outerRow, innerRow);
        Row rightRow = joinType.rightSide(outerRow, innerRow);
        if (buildOuterInput) {
            Row leftRowTemp = leftRow;
            leftRow = rightRow;
            rightRow = leftRowTemp;
        }
        JoinRow joinRow = new JoinRow(leftRow.getColNum(), leftRow, rightRow, null);

        return checkJoinCondition(joinRow);
    }

    private boolean isScalarInputRefCondition() {
        return condition instanceof ScalarFunctionExpression &&
            ((ScalarFunctionExpression) condition).isInputRefArgs();
    }

    @Override
    protected boolean outputNullRowInTime() {
        return !buildOuterInput;
    }

    enum JoinKeyType {
        LONG,
        INTEGER,
        MULTI_INTEGER,
        OTHER
    }

    public static class PartitionChunksIndex {
        private final ChunksIndex builderChunks;
        private final ChunksIndex builderKeyChunks;
        private final StampedLock lock;

        public PartitionChunksIndex() {
            this.builderChunks = new ChunksIndex();
            this.builderKeyChunks = new ChunksIndex();
            this.lock = new StampedLock();
        }

        public void appendChunk(Chunk chunk, Chunk keyChunk) {
            long stamp = lock.writeLock();
            try {
                builderChunks.addChunk(chunk);
                builderKeyChunks.addChunk(keyChunk);
            } finally {
                lock.unlockWrite(stamp);
            }
        }

        public ChunksIndex getBuilderChunks() {
            return builderChunks;
        }

        public ChunksIndex getBuilderKeyChunks() {
            return builderKeyChunks;
        }
    }
}
