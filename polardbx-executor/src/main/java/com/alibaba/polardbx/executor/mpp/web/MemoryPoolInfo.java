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

package com.alibaba.polardbx.executor.mpp.web;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.unmodifiableMap;

public final class MemoryPoolInfo {
    private final long maxBytes;
    private final Map<String, Long> queryMemoryReservations;

    @JsonCreator
    public MemoryPoolInfo(
        @JsonProperty("maxBytes") long maxBytes,
        @JsonProperty("queryMemoryReservations") Map<String, Long> queryMemoryReservations) {
        this.maxBytes = maxBytes;
        this.queryMemoryReservations = unmodifiableMap(new HashMap<>(queryMemoryReservations));
    }

    @JsonProperty
    public long getMaxBytes() {
        return maxBytes;
    }

    @JsonProperty
    public Map<String, Long> getQueryMemoryReservations() {
        return queryMemoryReservations;
    }
}
