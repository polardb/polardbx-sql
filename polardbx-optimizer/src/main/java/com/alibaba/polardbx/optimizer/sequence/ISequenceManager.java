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

package com.alibaba.polardbx.optimizer.sequence;

import com.alibaba.polardbx.common.constants.SequenceAttribute;
import com.alibaba.polardbx.common.constants.SequenceAttribute.Type;
import com.alibaba.polardbx.common.model.lifecycle.Lifecycle;

import java.util.Map;

/**
 * @author mengshi.sunmengshi 2014/04/28 14:51:04
 * @since 5.1.0
 */
public interface ISequenceManager extends Lifecycle {

    String AUTO_SEQ_PREFIX = SequenceAttribute.AUTO_SEQ_PREFIX;

    /**
     * Get next sequence value
     */
    Long nextValue(String schemaName, String seqName);

    /**
     * Get next batch of sequence values
     */
    Long nextValue(String schemaName, String seqName, int batchSize);

    /**
     * Get current sequence value
     */
    Long currValue(String schemaName, String seqName);

    /**
     * 消耗内存中的sequence
     */
    boolean exhaustValue(String schemaName, String seqName);

    /**
     * Update sequence value directly without calling stored procedure
     * nextSeqValue.
     */
    void updateValue(String schemaName, String seqName, long value);

    /**
     * Get current increment.
     */
    Integer getIncrement(String schemaName, String seqName);

    /**
     * Invalidate sequence object cached after DROP SEQUENCE.
     */
    void invalidate(String schemaName, String seqName);

    /**
     * Invalidate all cached sequence objects.
     */
    int invalidateAll(String schemaName);

    /**
     * Check if a sequence with the name exists. If not, then return Type.NA.
     */
    Type checkIfExists(String schemaName, String seqName);

    /**
     * Check if this table is using sequence.
     */
    boolean isUsingSequence(String schemaName, String tableName);

    /**
     * Return the native object of sequence. If not exists, then return null.
     */
    Object getSequence(String schemaName, String seqName);

    /**
     * Get current group sequence range.
     */
    String getCurrentSeqRange(String schemaName, String seqName);

    /**
     * Get minimum value from current group sequence range.
     */
    long getMinValueFromCurrentSeqRange(String schemaName, String seqName);

    /**
     * Get global custom unit arguments: unit_count, unit_index and inner_step
     * for reference of the creation of group sequence.
     */
    int[] getCustomUnitArgsForGroupSeq(String schemaName);

    /**
     * Check if all existing sequences are the same type specified.
     */
    boolean areAllSequencesSameType(String schemaName, Type seqType);

    /**
     * Reload connection properties so that the latest configuration
     * can take effect for Sequence.
     */
    void reloadConnProps(String schemaName, Map<String, Object> connProps);

    /**
     * Reset all New Sequence related queues and handlers.
     */
    void resetNewSeqResources(String schemaName);

}
