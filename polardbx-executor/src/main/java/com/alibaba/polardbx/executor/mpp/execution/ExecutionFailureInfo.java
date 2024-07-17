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
package com.alibaba.polardbx.executor.mpp.execution;

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.mpp.client.FailureInfo;
import com.alibaba.polardbx.executor.mpp.metadata.NotNull;
import com.alibaba.polardbx.executor.mpp.util.ImmutableCollectors;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;

@Immutable
public class ExecutionFailureInfo {
    private static final Pattern STACK_TRACE_PATTERN = Pattern.compile("(.*)\\.(.*)\\(([^:]*)(?::(.*))?\\)");

    private Throwable throwable;
    private final String type;
    private final String message;
    private final ExecutionFailureInfo cause;
    private final List<ExecutionFailureInfo> suppressed;
    private final List<String> stack;
    private final ErrorCode errorCode;

    public ExecutionFailureInfo(Throwable throwable,
                                String type,
                                String message,
                                ExecutionFailureInfo cause,
                                List<ExecutionFailureInfo> suppressed,
                                List<String> stack,
                                ErrorCode errorCode) {
        this(type, message, cause, suppressed, stack, errorCode);
        this.throwable = throwable;
    }

    @JsonCreator
    public ExecutionFailureInfo(
        @JsonProperty("type") String type,
        @JsonProperty("message") String message,
        @JsonProperty("cause") ExecutionFailureInfo cause,
        @JsonProperty("suppressed") List<ExecutionFailureInfo> suppressed,
        @JsonProperty("stack") List<String> stack,
        @JsonProperty("errorCode") @Nullable ErrorCode errorCode) {
        requireNonNull(type, "type is null");
        requireNonNull(suppressed, "suppressed is null");
        requireNonNull(stack, "stack is null");
        this.type = type;
        this.message = message;
        this.cause = cause;
        this.suppressed = ImmutableList.copyOf(suppressed);
        this.stack = ImmutableList.copyOf(stack);
        this.errorCode = errorCode;
    }

    @NotNull
    @JsonProperty
    public String getType() {
        return type;
    }

    @Nullable
    @JsonProperty
    public String getMessage() {
        return message;
    }

    @Nullable
    @JsonProperty
    public ExecutionFailureInfo getCause() {
        return cause;
    }

    @NotNull
    @JsonProperty
    public List<ExecutionFailureInfo> getSuppressed() {
        return suppressed;
    }

    @NotNull
    @JsonProperty
    public List<String> getStack() {
        return stack;
    }

    @Nullable
    @JsonProperty
    public ErrorCode getErrorCode() {
        return errorCode;
    }

    public FailureInfo toFailureInfo() {
        List<FailureInfo> suppressed = this.suppressed.stream()
            .map(ExecutionFailureInfo::toFailureInfo)
            .collect(ImmutableCollectors.toImmutableList());

        return new FailureInfo(throwable, type, message, cause == null ? null : cause.toFailureInfo(), suppressed,
            stack);
    }

    public Throwable toException() {
        if (throwable != null) {
            return throwable;
        }
        return toException(this);
    }

    private static Throwable toException(ExecutionFailureInfo executionFailureInfo) {
        if (executionFailureInfo == null) {
            return null;
        }

        Throwable failure;
        if (executionFailureInfo.getErrorCode() == null) {
            failure = new TddlNestableRuntimeException(executionFailureInfo.getMessage(),
                toException(executionFailureInfo.getCause()));
        } else {
            failure = new TddlRuntimeException(executionFailureInfo.getErrorCode(),
                executionFailureInfo.getMessage(),
                toException(executionFailureInfo.getCause()));
        }
        for (ExecutionFailureInfo suppressed : executionFailureInfo.getSuppressed()) {
            failure.addSuppressed(toException(suppressed));
        }
        ImmutableList.Builder<StackTraceElement> stackTraceBuilder = ImmutableList.builder();
        for (String stack : executionFailureInfo.getStack()) {
            stackTraceBuilder.add(toStackTraceElement(stack));
        }
        ImmutableList<StackTraceElement> stackTrace = stackTraceBuilder.build();
        failure.setStackTrace(stackTrace.toArray(new StackTraceElement[stackTrace.size()]));
        return failure;
    }

    private static StackTraceElement toStackTraceElement(String stack) {
        Matcher matcher = STACK_TRACE_PATTERN.matcher(stack);
        if (matcher.matches()) {
            String declaringClass = matcher.group(1);
            String methodName = matcher.group(2);
            String fileName = matcher.group(3);
            int number = -1;
            if (fileName.equals("Native Method")) {
                fileName = null;
                number = -2;
            } else if (matcher.group(4) != null) {
                number = Integer.parseInt(matcher.group(4));
            }
            return new StackTraceElement(declaringClass, methodName, fileName, number);
        }
        return new StackTraceElement("Unknown", stack, null, -1);
    }
}
