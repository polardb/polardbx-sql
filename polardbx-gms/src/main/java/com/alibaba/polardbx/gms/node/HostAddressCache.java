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

package com.alibaba.polardbx.gms.node;

import com.fasterxml.jackson.annotation.JsonCreator;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class HostAddressCache {

    private static HostAddressCache INSTANCE = new HostAddressCache();

    private static final int CACHE_SIZE = 512;

    private static Map<String, HostAddress> hostAddressMap = new ConcurrentHashMap<>();

    private static Function<String, HostAddress> hostAddressFunction = new Function<String, HostAddress>() {
        @Override
        public HostAddress apply(String s) {
            if (hostAddressMap.size() > CACHE_SIZE) {
                hostAddressMap.clear();
            }
            return HostAddress.instance(s);
        }
    };

    public static HostAddressCache getInstance() {
        return INSTANCE;
    }

    public HostAddress getHostAddress(String key) {
        return hostAddressMap.computeIfAbsent(key, hostAddressFunction);
    }

    /**
     * Return true for valid port numbers.
     */
    private static boolean isValidPort(int port) {
        return port >= 0 && port <= 65535;
    }

    /**
     * Build a HostAddress instance from separate host and port values.
     * <p>
     * <p>Note: Non-bracketed IPv6 literals are allowed.
     *
     * @param host the host string to parse.  Must not contain a port number.
     * @param port a port number from [0..65535]
     * @return if parsing was successful, a populated HostAddress object.
     * @throws IllegalArgumentException if {@code host} contains a port number,
     * or {@code port} is out of range.
     */
    public static HostAddress fromParts(String host, int port) {
        if (!isValidPort(port)) {
            throw new IllegalArgumentException("Port is invalid: " + port);
        }
        return hostAddressMap.computeIfAbsent(host + ":" + port, hostAddressFunction);
    }

    /**
     * Split a freeform string into a host and port, without strict validation.
     * <p>
     * Note that the host-only formats will leave the port field undefined.  You
     * can use {@link #withDefaultPort(int)} to patch in a default value.
     *
     * @param hostPortString the input string to parse.
     * @return if parsing was successful, a populated HostAddress object.
     * @throws IllegalArgumentException if nothing meaningful could be parsed.
     */
    @JsonCreator
    public static HostAddress fromString(String hostPortString) {
        return HostAddressCache.getInstance().getHostAddress(hostPortString);
    }

    public static HostAddress fromUri(URI httpUri) {
        String host = httpUri.getHost();
        int port = httpUri.getPort();
        if (port < 0) {
            return fromString(host);
        } else {
            return fromParts(host, port);
        }
    }

}
