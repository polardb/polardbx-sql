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

package com.alibaba.polardbx.executor.mpp.deploy;

/**
 * The Configure will be used by Bootstrap component.
 */
public class BootstrapConfig {
    public static final String CONFIG_KEY_NODE_ENV = "node.environment";
    public static final String CONFIG_KEY_NODE_ID = "node.id";
    public static final String CONFIG_KEY_HTTP_PORT = "http-server.http.port";
    public static final String CONFIG_KEY_HTTP_SERVER_LOG_ENABLED = "http-server.log.enabled";

    public static final String CONFIG_KEY_HTTP_SERVER_MAX_THREADS = "http-server.threads.max"; //200
    public static final String CONFIG_KEY_HTTP_SERVER_MIN_THREADS = "http-server.threads.min"; //2
}
