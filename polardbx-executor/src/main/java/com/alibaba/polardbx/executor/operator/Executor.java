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

import com.alibaba.polardbx.optimizer.chunk.Chunk;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;

import java.util.List;

/**
 * Basic interface for operators
 */
public interface Executor extends ProducerExecutor {

    /**
     * This method is called immediately before any chunk are processed, it should contain the
     * operator's initialization logic.
     *
     * @throws java.lang.Exception An exception in this method causes the operator to fail.
     */
    void open();

    /**
     * output the result
     */
    Chunk nextChunk();

    /**
     * This method is called after all records have been added to the operators via the methods
     */
    void close();

    List<DataType> getDataTypes();

    /**
     * get the inputs of operators
     */
    List<Executor> getInputs();

    /**
     * set the id which is flag for this operator.
     */
    default void setId(int id) {
        throw new UnsupportedOperationException(getClass().getName());
    }

    default int getId() {
        throw new UnsupportedOperationException(getClass().getName());
    }
}
