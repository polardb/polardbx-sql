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
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.util.MoreObjects;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import com.alibaba.polardbx.executor.mpp.metadata.NotNull;

@Immutable
public class QueryError {
    private final String message;
    private final String sqlState;
    private final int errorCode;
    private final String errorName;
    private final String errorType;
    private final FailureInfo failureInfo;

    @JsonCreator
    public QueryError(
        @JsonProperty("message") String message,
        @JsonProperty("sqlState") String sqlState,
        @JsonProperty("errorCode") int errorCode,
        @JsonProperty("errorName") String errorName,
        @JsonProperty("errorType") String errorType,
        @JsonProperty("failureInfo") FailureInfo failureInfo) {
        this.message = message;
        this.sqlState = sqlState;
        this.errorCode = errorCode;
        this.errorName = errorName;
        this.errorType = errorType;
        this.failureInfo = failureInfo;
    }

    @NotNull
    @JsonProperty
    public String getMessage() {
        return message;
    }

    @Nullable
    @JsonProperty
    public String getSqlState() {
        return sqlState;
    }

    @JsonProperty
    public int getErrorCode() {
        return errorCode;
    }

    @NotNull
    @JsonProperty
    public String getErrorName() {
        return errorName;
    }

    @NotNull
    @JsonProperty
    public String getErrorType() {
        return errorType;
    }

    @Nullable
    @JsonProperty
    public FailureInfo getFailureInfo() {
        return failureInfo;
    }

    public Throwable toException() {
        if (failureInfo != null) {
            return failureInfo.toException();
        }
        return new TddlRuntimeException(ErrorCode.valueOf(errorName), message);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("message", message)
            .add("sqlState", sqlState)
            .add("errorCode", errorCode)
            .add("errorName", errorName)
            .add("errorType", errorType)
            .add("failureInfo", failureInfo != null ? failureInfo.getMessage() : "")
            .toString();
    }
}
