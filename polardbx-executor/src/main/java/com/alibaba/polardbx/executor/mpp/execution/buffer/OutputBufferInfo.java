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

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.polardbx.executor.mpp.execution.buffer;

import com.alibaba.polardbx.util.MoreObjects;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public final class OutputBufferInfo {
    private final BufferState state;
    private final long totalBufferedBytes;

    @JsonCreator
    public OutputBufferInfo(
        @JsonProperty("state") BufferState state,
        @JsonProperty("totalBufferedBytes") long totalBufferedBytes) {
        this.state = state;
        this.totalBufferedBytes = totalBufferedBytes;
    }

    @JsonProperty
    public BufferState getState() {
        return state;
    }

    @JsonProperty
    public long getTotalBufferedBytes() {
        return totalBufferedBytes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        OutputBufferInfo that = (OutputBufferInfo) o;
        return Objects.equals(totalBufferedBytes, that.totalBufferedBytes) &&
            Objects.equals(state, that.state);
    }

    @Override
    public int hashCode() {
        int hash = 1;
        hash = hash * 31 + (state == null ? 0 : state.hashCode());
        hash = hash * 31 + Long.hashCode(totalBufferedBytes);
        return hash;
        //return Objects.hash(state, canAddBuffers, canAddPages, totalBufferedBytes, totalBufferedPages, totalRowsSent, totalPagesSent, buffers);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("state", state)
            .add("totalBufferedBytes", totalBufferedBytes)
            .toString();
    }
}
