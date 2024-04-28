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
package com.alibaba.polardbx.executor.mpp.metadata;

import com.alibaba.polardbx.executor.mpp.spi.ConnectorSplit;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class Split {

    public static final Split EMPTY_SPLIT = new Split(true, null);
    protected final boolean remoteSplit;
    protected final ConnectorSplit connectorSplit;

    @JsonCreator
    public Split(
        @JsonProperty("remoteSplit") boolean remoteSplit,
        @JsonProperty("connectorSplit") ConnectorSplit connectorSplit) {
        this.remoteSplit = requireNonNull(remoteSplit, "remoteSplit is null");
        this.connectorSplit = connectorSplit;
    }

    @JsonProperty
    public boolean isRemoteSplit() {
        return remoteSplit;
    }

    @JsonProperty
    public ConnectorSplit getConnectorSplit() {
        return connectorSplit;
    }

    @Override
    public String toString() {
        return toStringHelper(this)
            .add("isRemoteSplit", remoteSplit)
            .add("connectorSplit", connectorSplit)
            .toString();
    }

    public Split copyWithSplit(ConnectorSplit connectorSplit) {
        return new Split(remoteSplit, connectorSplit);
    }
}
