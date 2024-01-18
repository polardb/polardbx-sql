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

package com.alibaba.polardbx.optimizer.core.planner.rule.util;

/**
 * @author chenghui.lch
 */
public class ExecutionStrategyResult {

    /**
     * The final execution strategy for dml
     */
    public ExecutionStrategy execStrategy = null;

    /**
     * Specify the execution strategy by connectionProperties
     */
    public boolean useStrategyByHintParams = false;

    /**
     * The flag that if the dml can push duplicate check
     */
    public boolean canPushDuplicateIgnoreScaleOutCheck = true;

    /**
     * Specify the push of DuplicateCheck by connectionProperties
     * When
     * ConnectionParams.DML_PUSH_DUPLICATE_CHECK is true:
     * push replace/upsert/insertIgnore directly when its target table has no gsi or scale-out,
     * <p>
     * ConnectionParams.DML_PUSH_DUPLICATE_CHECK is false:
     * only push replace/upsert/insertIgnore directly when its sql can be push
     */
    public boolean pushDuplicateCheckByHintParams = false;

    /**
     * The flag that if the dml need do multi-write
     */
    public boolean doMultiWrite = false;

    public boolean pushablePrimaryKeyCheck = true;

    public boolean pushableForeignConstraintCheck = true;
}
