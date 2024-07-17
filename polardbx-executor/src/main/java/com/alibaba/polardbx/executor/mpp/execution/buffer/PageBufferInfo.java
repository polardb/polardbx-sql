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

public class PageBufferInfo {
    private final int partition;
    private final long bufferedBytes;

    @JsonCreator
    public PageBufferInfo(
        @JsonProperty("partition") int partition,
        @JsonProperty("bufferedBytes") long bufferedBytes) {
        this.partition = partition;
        this.bufferedBytes = bufferedBytes;
    }

    @JsonProperty
    public int getPartition() {
        return partition;
    }

    @JsonProperty
    public long getBufferedBytes() {
        return bufferedBytes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PageBufferInfo that = (PageBufferInfo) o;
        return Objects.equals(partition, that.partition) &&
            Objects.equals(bufferedBytes, that.bufferedBytes);
    }

    @Override
    public int hashCode() {
        int hash = 1;
        hash = hash * 31 + Integer.hashCode(partition);
        hash = hash * 31 + Long.hashCode(bufferedBytes);
        return hash;
        //return Objects.hash(partition, bufferedPages, bufferedBytes, rowsAdded, pagesAdded);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("partition", partition)
            .add("bufferedBytes", bufferedBytes)
            .toString();
    }

    public static PageBufferInfo empty() {
        return new PageBufferInfo(0, 0);
    }
}
