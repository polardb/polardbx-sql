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

import com.alibaba.polardbx.optimizer.core.datatype.DataType;

/**
 * Random accessible column vector, extended by Block class.
 */
public interface RandomAccessBlock extends CastableBlock {

    /**
     * Set the nullness array to the vector
     *
     * @param isNull the nullness array.
     */
    void setIsNull(boolean[] isNull);

    /**
     * TODO should merge with mayHaveNull()
     * Check if the block has null values according to has null flag
     *
     * @return TRUE if has null.
     */
    boolean hasNull();

    /**
     * Set has null flag.
     */
    void setHasNull(boolean hasNull);

    /**
     * Get the type of the vector.
     */
    DataType getType();

    /**
     * Get the nullness array.
     */
    boolean[] nulls();

    /**
     * Get the data digest of this block
     */
    String getDigest();

    /**
     * Copy the data from this vector to destination vector
     *
     * @param selectedInUse if selection array in use.
     * @param sel selection array.
     * @param size data size.
     * @param output output vector.
     */
    void copySelected(boolean selectedInUse, int[] sel, int size, RandomAccessBlock output);

    /**
     * Copy the main data to the destination vector.
     *
     * @param another the destination vector.
     */
    void shallowCopyTo(RandomAccessBlock another);

    /**
     * Get the value in specific position.
     *
     * @param position data position.
     * @return The value in specific position, may be null.
     */
    Object elementAt(int position);

    /**
     * Set the value in specific position.
     *
     * @param position data position.
     * @param element value to set, may be null.
     */
    void setElementAt(int position, Object element);

    /**
     * Reset the size of this block.
     *
     * @param positionCount new block size.
     */
    void resize(int positionCount);

    /**
     * Compact the selected block to unselected block.
     *
     * @param selection selection array.
     */
    default void compact(int[] selection) {
    }
}
