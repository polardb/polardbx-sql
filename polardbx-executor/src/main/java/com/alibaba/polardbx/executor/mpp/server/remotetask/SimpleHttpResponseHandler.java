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
package com.alibaba.polardbx.executor.mpp.server.remotetask;

import com.google.common.util.concurrent.FutureCallback;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import io.airlift.http.client.FullJsonResponseHandler;
import io.airlift.http.client.HttpStatus;

import java.net.URI;

import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_REMOTE_TASK;
import static java.lang.String.format;

public class SimpleHttpResponseHandler<T>
    implements FutureCallback<FullJsonResponseHandler.JsonResponse<T>> {
    private final SimpleHttpResponseCallback<T> callback;

    private final URI uri;

    public SimpleHttpResponseHandler(SimpleHttpResponseCallback<T> callback, URI uri) {
        this.callback = callback;
        this.uri = uri;
    }

    @Override
    public void onSuccess(FullJsonResponseHandler.JsonResponse<T> response) {
        try {
            if (response.getStatusCode() == HttpStatus.OK.code() && response.hasValue()) {
                callback.success(response.getValue());
            } else if (response.getStatusCode() == HttpStatus.SERVICE_UNAVAILABLE.code()) {
                callback.failed(new ServiceUnavailableException(uri));
            } else {
                // Something is broken in the server or the client, so fail the task immediately (includes 500 errors)
                Exception cause = response.getException();
                if (cause == null) {
                    if (response.getStatusCode() == HttpStatus.OK.code()) {
                        cause = new TddlRuntimeException(ERR_REMOTE_TASK,
                            format("Expected response from %s is empty", uri));
                    } else {
                        cause = new TddlRuntimeException(ERR_REMOTE_TASK,
                            format("Expected response code from %s to be %s, but was %s%n%s",
                                uri,
                                HttpStatus.OK.code(),
                                response.getStatusCode(),
                                response.getResponseBody()));

                    }
                }
                callback.fatal(cause);
            }
        } catch (Throwable t) {
            // this should never happen
            callback.fatal(t);
        }
    }

    @Override
    public void onFailure(Throwable t) {
        callback.failed(t);
    }

    private static class ServiceUnavailableException extends RuntimeException {

        public ServiceUnavailableException(URI uri) {
            super("Server returned SERVICE_UNAVAILABLE: " + uri);
        }
    }
}
