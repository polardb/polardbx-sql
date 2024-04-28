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
package com.alibaba.polardbx.executor.mpp.util;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.mpp.execution.ExecutionFailureInfo;
import com.alibaba.polardbx.executor.mpp.execution.Failure;
import com.google.common.collect.Lists;

import javax.annotation.Nullable;
import java.net.ConnectException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;

import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_SERVER_SHUTTING_DOWN;
import static com.alibaba.polardbx.util.MoreObjects.firstNonNull;
import static com.google.common.base.Functions.toStringFunction;
import static java.lang.String.format;
import static java.util.Arrays.asList;

public final class Failures {
    private static final String NODE_CRASHED_ERROR = "The node may have crashed or be under too much load. " +
        "This is probably a transient issue, so please retry your query in a few minutes.";

    public static final String WORKER_NODE_ERROR =
        "Encountered too many errors talking to a worker node. " + NODE_CRASHED_ERROR;

    public static final String REMOTE_TASK_MISMATCH_ERROR =
        "Could not communicate with the remote task. " + NODE_CRASHED_ERROR;

    private static final String CONNECTION_REFUSED_MESSAGE = "Connection refused";

    private Failures() {
    }

    public static ExecutionFailureInfo toFailure(Throwable failure) {
        if (failure == null) {
            return null;
        }
        // todo prevent looping with suppressed cause loops and such
        String type;
        if (failure instanceof Failure) {
            type = ((Failure) failure).getType();
        } else {
            Class<?> clazz = failure.getClass();
            type = firstNonNull(clazz.getCanonicalName(), clazz.getName());
        }

        return new ExecutionFailureInfo(failure,
            type,
            failure.getMessage(),
            toFailure(failure.getCause()),
            toFailures(asList(failure.getSuppressed())),
            Lists.transform(asList(failure.getStackTrace()), toStringFunction()),
            toErrorCode(failure));
    }

    public static void checkCondition(boolean condition, ErrorCode errorCode, String formatString, Object... args) {
        if (!condition) {
            throw new TddlRuntimeException(errorCode, format(formatString, args));
        }
    }

    public static List<ExecutionFailureInfo> toFailures(Collection<? extends Throwable> failures) {
        return failures.stream()
            .map(Failures::toFailure)
            .collect(ImmutableCollectors.toImmutableList());
    }

    @Nullable
    private static ErrorCode toErrorCode(@Nullable Throwable throwable) {
        if (throwable == null) {
            return null;
        }

        if (throwable instanceof TddlRuntimeException) {
            return ((TddlRuntimeException) throwable).getErrorCodeType();
        }
        if (throwable instanceof Failure && ((Failure) throwable).getErrorCode() != null) {
            return ((Failure) throwable).getErrorCode();
        }
        if (throwable.getCause() != null) {
            return toErrorCode(throwable.getCause());
        }
        return null;
    }

    public static boolean isConnectionRefused(Throwable t) {
        if (t != null && t instanceof RejectedExecutionException) {
            return true;
        }

        while (t != null) {
            if (t instanceof ConnectException
                && t.getMessage() != null && t.getMessage().indexOf(CONNECTION_REFUSED_MESSAGE) != -1) {
                return true;
            }
            ErrorCode errorCode = null;
            if (t instanceof TddlRuntimeException) {
                errorCode = ((TddlRuntimeException) t).getErrorCodeType();
            } else if (t instanceof Failure) {
                errorCode = ((Failure) t).getErrorCode();
            }
            if (errorCode != null && errorCode.getCode() == ERR_SERVER_SHUTTING_DOWN.getCode()) {
                return true;
            }
            t = t.getCause();
        }
        return false;
    }
}
