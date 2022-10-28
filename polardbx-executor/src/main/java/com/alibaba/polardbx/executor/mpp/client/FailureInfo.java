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
import com.google.common.collect.ImmutableList;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import com.alibaba.polardbx.executor.mpp.metadata.NotNull;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;

@Immutable
public class FailureInfo {
    private static final Pattern STACK_TRACE_PATTERN = Pattern.compile("(.*)\\.(.*)\\(([^:]*)(?::(.*))?\\)");

    private Throwable throwable;

    private final String type;
    private final String message;
    private final FailureInfo cause;
    private final List<FailureInfo> suppressed;
    private final List<String> stack;

    public FailureInfo(Throwable throwable,
                       String type,
                       String message,
                       FailureInfo cause,
                       List<FailureInfo> suppressed,
                       List<String> stack) {
        this(type, message, cause, suppressed, stack);
        this.throwable = throwable;
    }

    @JsonCreator
    public FailureInfo(
        @JsonProperty("type") String type,
        @JsonProperty("message") String message,
        @JsonProperty("cause") FailureInfo cause,
        @JsonProperty("suppressed") List<FailureInfo> suppressed,
        @JsonProperty("stack") List<String> stack) {
        requireNonNull(type, "type is null");
        requireNonNull(suppressed, "suppressed is null");
        requireNonNull(stack, "stack is null");

        this.type = type;
        this.message = message;
        this.cause = cause;
        this.suppressed = ImmutableList.copyOf(suppressed);
        this.stack = ImmutableList.copyOf(stack);
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
    public FailureInfo getCause() {
        return cause;
    }

    @NotNull
    @JsonProperty
    public List<FailureInfo> getSuppressed() {
        return suppressed;
    }

    @NotNull
    @JsonProperty
    public List<String> getStack() {
        return stack;
    }

    public Throwable toExceptionWithoutType() {
        return toException(this);
    }

    public Throwable toException() {
        if (throwable != null) {
            return throwable;
        }
        return toException(this);
    }

    @Override
    public String toString() {
        return "FailureInfo{" +
            "message='" + message + '\'' +
            ", stack=\n" + String.join("\n", stack) +
            '}';
    }

    private static Throwable toException(FailureInfo failureInfo) {
        if (failureInfo == null) {
            return null;
        }
        RuntimeException failure =
            new TddlNestableRuntimeException(failureInfo.getMessage(), toException(failureInfo.getCause()));
        for (FailureInfo suppressed : failureInfo.getSuppressed()) {
            failure.addSuppressed(toException(suppressed));
        }
        ImmutableList.Builder<StackTraceElement> stackTraceBuilder = ImmutableList.builder();
        for (String stack : failureInfo.getStack()) {
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
