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
package com.alibaba.polardbx.executor.mpp.client;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.alibaba.polardbx.util.MoreObjects;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import javax.validation.constraints.NotNull;
import java.net.URI;

import static com.google.common.collect.Iterables.unmodifiableIterable;
import static java.util.Objects.requireNonNull;

@Immutable
public class QueryResults {
    private final String id;
    private final URI nextUri;
    private final Iterable<Object> data;
    private final QueryError error;
    private final Long updateCount;
    private long token;

    @JsonCreator
    public QueryResults(
        @JsonProperty("id") String id,
        @JsonProperty("nextUri") URI nextUri,
        @JsonProperty("data") Iterable<Object> data,
        @JsonProperty("error") QueryError error,
        @JsonProperty("updateCount") Long updateCount,
        @JsonProperty("token") Long token) {
        this.id = requireNonNull(id, "id is null");
        this.nextUri = nextUri;
        this.data = (data != null) ? unmodifiableIterable(data) : null;
        this.error = error;
        this.updateCount = updateCount;
        this.token = token;
    }

    @NotNull
    @JsonProperty
    public String getId() {
        return id;
    }

    @Nullable
    @JsonProperty
    public URI getNextUri() {
        return nextUri;
    }

    @Nullable
    @JsonProperty
    public Iterable<Object> getData() {
        return data;
    }

    @Nullable
    @JsonProperty
    public QueryError getError() {
        return error;
    }

    @Nullable
    @JsonProperty
    public Long getUpdateCount() {
        return updateCount;
    }

    @Nullable
    @JsonProperty
    public Long getToken() {
        return token;
    }

    public void setToken(long token) {
        this.token = token;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("id", id)
            .add("nextUri", nextUri)
            .add("hasData", data != null)
            .add("error", error)
            .add("updateCount", updateCount)
            .add("token", token)
            .toString();
    }
}
