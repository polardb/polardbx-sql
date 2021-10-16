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

package com.alibaba.polardbx.executor.mpp.web;

import io.airlift.http.client.Request;
import io.airlift.http.client.ResponseHandler;

import java.io.IOException;
import java.io.InputStream;

public class StreamingJsonResponseHandler
    implements ResponseHandler<InputStream, RuntimeException> {
    @Override
    public InputStream handleException(Request request, Exception exception) {
        throw new RuntimeException("Request to worker failed", exception);
    }

    @Override
    public InputStream handle(Request request, io.airlift.http.client.Response response) {
        try {
            return response.getInputStream();
        } catch (IOException e) {
            throw new RuntimeException("Unable to read response from worker", e);
        }
    }
}