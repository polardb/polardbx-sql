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

import com.alibaba.polardbx.executor.mpp.OutputBuffers;
import com.alibaba.polardbx.util.MoreObjects;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class BufferInfo {
    private final OutputBuffers.OutputBufferId bufferId;
    private final boolean finished;
    private final PageBufferInfo pageBufferInfo;

    @JsonCreator
    public BufferInfo(
        @JsonProperty("bufferId") OutputBuffers.OutputBufferId bufferId,
        @JsonProperty("finished") boolean finished,
        @JsonProperty("pageBufferInfo") PageBufferInfo pageBufferInfo) {
        this.bufferId = requireNonNull(bufferId, "bufferId is null");
        this.pageBufferInfo = requireNonNull(pageBufferInfo, "pageBufferInfo is null");
        this.finished = finished;
    }

    @JsonProperty
    public OutputBuffers.OutputBufferId getBufferId() {
        return bufferId;
    }

    @JsonProperty
    public boolean isFinished() {
        return finished;
    }

    @JsonProperty
    public PageBufferInfo getPageBufferInfo() {
        return pageBufferInfo;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BufferInfo that = (BufferInfo) o;
        return Objects.equals(finished, that.finished) &&
            Objects.equals(bufferId, that.bufferId) &&
            Objects.equals(pageBufferInfo, that.pageBufferInfo);
    }

    @Override
    public int hashCode() {
        int hash = 1;
        hash = hash * 31 + bufferId.hashCode();
        hash = hash * 31 + Boolean.hashCode(finished);
        hash = hash * 31 + pageBufferInfo.hashCode();
        return hash;
        //return Objects.hash(bufferId, finished, bufferedPages, pagesSent, pageBufferInfo);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("bufferId", bufferId)
            .add("finished", finished)
            .add("pageBufferInfo", pageBufferInfo)
            .toString();
    }
}
