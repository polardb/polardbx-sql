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
package com.alibaba.polardbx.gms.node;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.Objects;

/**
 * An immutable representation of a host and port.
 * <p>
 * <p>Example usage:
 * <pre>
 * HostAndPort hp = HostAndPort.fromString("[2001:db8::1]")
 *     .withDefaultPort(80)
 *     .requireBracketsForIPv6();
 * hp.getHostText();  // returns "2001:db8::1"
 * hp.getPort();      // returns 80
 * hp.toString();     // returns "[2001:db8::1]:80"
 * </pre>
 * <p>
 * <p>Here are some examples of recognized formats:
 * <ul>
 * <li>example.com
 * <li>example.com:80
 * <li>192.0.2.1
 * <li>192.0.2.1:80
 * <li>[2001:db8::1]     - {@link #getHostText()} omits brackets
 * <li>[2001:db8::1]:80  - {@link #getHostText()} omits brackets
 * <li>2001:db8::1
 * </ul>
 * <p>
 * <p>Note that this is not an exhaustive list, because these methods are only
 * concerned with brackets, colons, and port numbers.  Full validation of the
 * host field (if desired) is the caller's responsibility.
 *
 * @author Paul Marks
 * @since 10.0
 */
public class HostAddress {
    /**
     * Magic value indicating the absence of a port number.
     */
    private static final int NO_PORT = -1;

    /**
     * Hostname, IPv4/IPv6 literal, or unvalidated nonsense.
     */
    private final String host;

    /**
     * Validated port number in the range [0..65535], or NO_PORT
     */
    private final int port;

    public HostAddress(String host, int port) {
        this.host = host;
        this.port = port;
    }

    /**
     * Returns the portion of this {@code HostAddress} instance that should
     * represent the hostname or IPv4/IPv6 literal.
     * <p>
     * <p>A successful parse does not imply any degree of sanity in this field.
     * For additional validation, see the {@link com.google.common.net.HostSpecifier} class.
     */
    public String getHostText() {
        return host;
    }

    /**
     * Return true if this instance has a defined port.
     */
    public boolean hasPort() {
        return port >= 0;
    }

    /**
     * Get the current port number, failing if no port is defined.
     *
     * @return a validated port number, in the range [0..65535]
     * @throws IllegalStateException if no port is defined.  You can use
     */
    public int getPort() {
        if (!hasPort()) {
            throw new IllegalStateException("no port");
        }
        return port;
    }

    @Override
    public int hashCode() {
        int hashCode = 1;
        hashCode = hashCode * 31 + host.hashCode();
        hashCode = hashCode * 31 + port;
        return hashCode;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final HostAddress other = (HostAddress) obj;
        return Objects.equals(this.host, other.host) &&
            Objects.equals(this.port, other.port);
    }

    /**
     * Rebuild the host:port string, including brackets if necessary.
     */
    @JsonValue
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder(host.length() + 7);
        if (host.indexOf(':') >= 0) {
            builder.append('[').append(host).append(']');
        } else {
            builder.append(host);
        }
        if (hasPort()) {
            builder.append(':').append(port);
        }
        return builder.toString();
    }

    /**
     * Return true for valid port numbers.
     */
    private static boolean isValidPort(int port) {
        return port >= 0 && port <= 65535;
    }

    public static HostAddress instance(String hostPortString) {
        if (hostPortString == null) {
            throw new NullPointerException("hostPortString is null");
        }
        String host;
        String portString = null;

        int colonPos = hostPortString.indexOf(':');
        if (colonPos >= 0 && hostPortString.indexOf(':', colonPos + 1) == -1) {
            // Exactly 1 colon.  Split into host:port.
            host = hostPortString.substring(0, colonPos);
            portString = hostPortString.substring(colonPos + 1);
        } else {
            // 0 or 2+ colons.  Bare hostname or IPv6 literal.
            host = hostPortString;
        }

        int port = NO_PORT;
        if (portString != null && portString.length() != 0) {
            // Try to parse the whole port string as a number.
            // JDK7 accepts leading plus signs. We don't want to.
            if (portString.startsWith("+")) {
                throw new IllegalArgumentException("Unparseable port number: " + hostPortString);
            }
            try {
                port = Integer.parseInt(portString);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Unparseable port number: " + hostPortString);
            }
            if (!isValidPort(port)) {
                throw new IllegalArgumentException("Port number out of range: " + hostPortString);
            }
        }

        return new HostAddress(host, port);
    }

    @JsonCreator
    public static HostAddress fromString(String hostPortString) {
        return HostAddressCache.getInstance().getHostAddress(hostPortString);
    }
}
