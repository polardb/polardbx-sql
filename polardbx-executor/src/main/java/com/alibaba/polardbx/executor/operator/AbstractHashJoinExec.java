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

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.optimizer.chunk.Chunk;
import com.alibaba.polardbx.executor.operator.util.ConcurrentRawHashTable;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.expression.calc.IExpression;
import com.alibaba.polardbx.optimizer.core.join.EquiJoinKey;
import com.alibaba.polardbx.common.utils.bloomfilter.FastIntBloomFilter;
import org.apache.calcite.rel.core.JoinRelType;

import java.util.List;

/**
 * Hash Join Executor
 *
 */
public abstract class AbstractHashJoinExec extends AbstractBufferedJoinExec implements ConsumerExecutor {

    private static final Logger logger = LoggerFactory.getLogger(AbstractHashJoinExec.class);

    /**
     * A placeholder to mark there is no more element in this position link
     */
    public static final int LIST_END = ConcurrentRawHashTable.NOT_EXISTS;

    ConcurrentRawHashTable hashTable;
    int[] positionLinks;
    FastIntBloomFilter bloomFilter;

    public AbstractHashJoinExec(Executor outerInput,
                                Executor innerInput,
                                JoinRelType joinType,
                                boolean maxOneRow,
                                List<EquiJoinKey> joinKeys,
                                IExpression otherCondition,
                                List<IExpression> antiJoinOperands,
                                ExecutionContext context) {
        super(outerInput, innerInput, joinType, maxOneRow, joinKeys, otherCondition, antiJoinOperands, null,
            context);
    }

    @Override
    public void closeConsume(boolean force) {
        buildChunks = null;
        buildKeyChunks = null;
        if (memoryPool != null) {
            collectMemoryUsage(memoryPool);
            memoryPool.destroy();
        }
    }

    @Override
    void doClose() {
        getProbeInput().close();
        closeConsume(true);

        this.hashTable = null;
        this.positionLinks = null;
    }

    @Override
    int matchInit(Chunk keyChunk, int[] hashCodes, int position) {
        int hashCode = hashCodes[position];
        if (bloomFilter != null && !bloomFilter.mightContain(hashCode)) {
            return LIST_END;
        }

        int matchedPosition = hashTable.get(hashCode);
        while (matchedPosition != LIST_END) {
            if (buildKeyChunks.equals(matchedPosition, keyChunk, position)) {
                break;
            }
            matchedPosition = positionLinks[matchedPosition];
        }
        return matchedPosition;
    }

    @Override
    int matchNext(int current, Chunk keyChunk, int position) {
        int matchedPosition = positionLinks[current];
        while (matchedPosition != LIST_END) {
            if (buildKeyChunks.equals(matchedPosition, keyChunk, position)) {
                break;
            }
            matchedPosition = positionLinks[matchedPosition];
        }
        return matchedPosition;
    }

    @Override
    boolean matchValid(int current) {
        return current != LIST_END;
    }
}
