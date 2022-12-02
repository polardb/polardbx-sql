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

package com.alibaba.polardbx.common.ddl.newengine;

import com.google.common.collect.ImmutableMap;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

public enum DdlState {

    QUEUED,
    RUNNING,
    PAUSED,
    COMPLETED,
    ROLLBACK_RUNNING,
    ROLLBACK_PAUSED,
    ROLLBACK_COMPLETED;

    public static final Set<DdlState> ALL_STATES = EnumSet.of(
        QUEUED,
        RUNNING,
        PAUSED,
        COMPLETED,
        ROLLBACK_RUNNING,
        ROLLBACK_PAUSED,
        ROLLBACK_COMPLETED
    );

    public static final Set<DdlState> RUNNABLE = EnumSet.of(QUEUED, RUNNING, ROLLBACK_RUNNING);

    public static final Set<DdlState> TERMINATED = EnumSet.of(ROLLBACK_PAUSED, PAUSED);

    public static final Set<DdlState> FINISHED = EnumSet.of(ROLLBACK_COMPLETED, COMPLETED);

    public static final Set<DdlState> PAUSED_POLICY_VALUES = EnumSet.of(PAUSED, RUNNING, ROLLBACK_RUNNING);

    public static final Set<DdlState> ROLLBACK_PAUSED_POLICY_VALUES = EnumSet.of(ROLLBACK_PAUSED, ROLLBACK_RUNNING);

    /**
     * State transition that run a job
     */
    public static final Map<DdlState, DdlState> RECOVER_JOB_STATE_TRANSFER =
        new ImmutableMap.Builder<DdlState, DdlState>()
            .put(DdlState.QUEUED, DdlState.RUNNING)
            .put(DdlState.PAUSED, DdlState.RUNNING)
            .put(DdlState.RUNNING, DdlState.RUNNING)
            .put(DdlState.COMPLETED, DdlState.COMPLETED)
            .put(DdlState.ROLLBACK_PAUSED, DdlState.ROLLBACK_RUNNING)
            .put(DdlState.ROLLBACK_RUNNING, DdlState.ROLLBACK_RUNNING)
            .put(DdlState.ROLLBACK_COMPLETED, DdlState.ROLLBACK_COMPLETED)
            .build();

    /**
     * State transition that rollback a job
     */
    public static final Map<DdlState, DdlState> ROLLBACK_JOB_STATE_TRANSFER =
        new ImmutableMap.Builder<DdlState, DdlState>()
            .put(DdlState.QUEUED, DdlState.ROLLBACK_COMPLETED)
            .put(DdlState.RUNNING, DdlState.ROLLBACK_RUNNING)
            .put(DdlState.PAUSED, DdlState.ROLLBACK_RUNNING)
            .put(DdlState.ROLLBACK_RUNNING, DdlState.ROLLBACK_RUNNING)
            .put(DdlState.ROLLBACK_PAUSED, DdlState.ROLLBACK_RUNNING)
            .build();

    /**
     * State transition that pause a job
     */
    public static final Map<DdlState, DdlState> PAUSE_JOB_STATE_TRANSFER =
        new ImmutableMap.Builder<DdlState, DdlState>()
            .put(DdlState.RUNNING, DdlState.PAUSED)
            .put(DdlState.QUEUED, DdlState.PAUSED)
            .put(DdlState.ROLLBACK_RUNNING, DdlState.ROLLBACK_PAUSED)
            .build();

    public static DdlState tryParse(String str, DdlState defaultState) {
        try {
            return valueOf(str);
        } catch (Exception e) {
            return defaultState;
        }
    }
}
