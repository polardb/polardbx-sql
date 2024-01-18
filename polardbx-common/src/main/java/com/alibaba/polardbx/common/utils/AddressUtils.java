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

package com.alibaba.polardbx.common.utils;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.google.common.base.Splitter;
import lombok.Data;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.SocketException;
import java.util.Enumeration;
import java.util.List;
import java.util.regex.Pattern;

public class AddressUtils {

    private static final Logger logger = LoggerFactory.getLogger(AddressUtils.class);
    private static final String LOCALHOST_IP = "127.0.0.1";
    private static final String EMPTY_IP = "0.0.0.0";
    private static final Pattern IP_PATTERN = Pattern.compile("[0-9]{1,3}(\\.[0-9]{1,3}){3,}");
    private static final Integer PAXOS_PORT_DELTA = 8000;

    @Data
    public static class XAddress {
        String ip;
        int port;
        int xport;

        public static XAddress parse(String addr) {
            List<String> xs = Splitter.on(":").splitToList(addr);
            if (xs.size() < 2) {
                throw new IllegalArgumentException("illegal x-address: " + addr);
            }
            XAddress res = new XAddress();
            res.setIp(xs.get(0));
            res.setPort(Integer.parseInt(xs.get(1)));
            if (xs.size() > 2) {
                res.setXport(Integer.parseInt(xs.get(2)));
            } else {
                res.setXport(0);
            }
            return res;
        }

        @Override
        public String toString() {
            return String.format("%s:%d:%d", ip, port, xport);
        }
    }

    public static boolean isAvailablePort(int port) {
        ServerSocket ss = null;
        try {
            ss = new ServerSocket(port);
            ss.bind(null);
            return true;
        } catch (IOException e) {
            return false;
        } finally {
            if (ss != null) {
                try {
                    ss.close();
                } catch (IOException e) {
                }
            }
        }
    }

    private static boolean isValidHostAddress(InetAddress address) {
        if (address == null || address.isLoopbackAddress()) {
            return false;
        }
        String name = address.getHostAddress();
        return isIpValid(name);
    }

    private static boolean isIpValid(String name) {
        return (name != null && !EMPTY_IP.equals(name) && !LOCALHOST_IP.equals(name) && IP_PATTERN.matcher(name)
            .matches());
    }

    public static String getHostIp() {
        InetAddress address = getHostAddress();
        return address == null ? null : address.getHostAddress();
    }

    public static String getHostName() {
        InetAddress address = getHostAddress();
        return address == null ? null : address.getHostName();
    }

    public static Pair<String, Integer> getIpPortPairByAddrStr(String addrStr) {
        String[] ipPortArr = addrStr.trim().split(":");
        assert ipPortArr.length == 2;
        String host = ipPortArr[0].trim();
        Integer port = Integer.valueOf(ipPortArr[1].trim());
        return new Pair<>(host, port);
    }

    public static XAddress resolveXAddress(String addr) {
        return XAddress.parse(addr);
    }

    public static String getAddrStrByIpPort(String host, Integer port) {
        StringBuilder addrSb = new StringBuilder();
        addrSb.append(host.trim()).append(":").append(port);
        String addrStr = addrSb.toString();
        return addrStr;
    }

    public static String joinHostPort(Pair<String, Integer> hostport) {
        return String.format("%s:%d", hostport.getKey(), hostport.getValue());
    }

    private static Integer getStorageNodePortByPaxosNodePort(Integer paxosPort) {
        return paxosPort - AddressUtils.PAXOS_PORT_DELTA;
    }

    public static String getStorageNodeAddrByPaxosNodeAddr(String paxosAddr) {
        Pair<String, Integer> paxosIpPort = getIpPortPairByAddrStr(paxosAddr);
        Pair<String, Integer> storageIpPort =
            new Pair<>(paxosIpPort.getKey(), getStorageNodePortByPaxosNodePort(paxosIpPort.getValue()));
        String storageAddr = getAddrStrByIpPort(storageIpPort.getKey(), storageIpPort.getValue());
        return storageAddr;
    }

    public static Pair<String, Integer> getPaxosAddressByStorageAddress(String ip, int port) {
        return Pair.of(ip, port + AddressUtils.PAXOS_PORT_DELTA);
    }

    public static String getPaxosAddressByStorageAddress(String storageAddr) {
        Pair<String, Integer> hostPort = getIpPortPairByAddrStr(storageAddr);
        Pair<String, Integer> paxosHostPort = getPaxosAddressByStorageAddress(hostPort.getKey(), hostPort.getValue());
        return joinHostPort(paxosHostPort);
    }

    public static InetAddress localAddress = null;
    public final static String POD_IP = "POD_IP";

    public static InetAddress getHostAddress() {
        if (localAddress != null) {
            return localAddress;
        }

        try {
            String podIp = System.getenv(POD_IP);
            if (!StringUtils.isBlank(podIp)) {
                podIp = podIp.trim();
                if (!isIpValid(podIp)) {
                    logger.error("pod ip not empty but not valid, pod ip is: " + podIp);
                } else {
                    localAddress = getMatchedAddress(podIp);
                    if (localAddress != null) {
                        logger.info("get matched address from pod ip: " + podIp);
                        return localAddress;
                    } else {
                        // local address should not null
                        logger.error("Failed to matched pod ip to address.");
                    }
                }
            }
            localAddress = InetAddress.getLocalHost();
            if (isValidHostAddress(localAddress)) {
                return localAddress;
            }
        } catch (Throwable e) {
            logger.warn("Failed to retriving local host ip address, try scan network card ip address. cause: "
                + e.getMessage());
        }
        try {
            Enumeration<NetworkInterface> interfaces = getNetInterface();
            if (interfaces != null) {
                while (interfaces.hasMoreElements()) {
                    try {
                        NetworkInterface network = interfaces.nextElement();
                        Enumeration<InetAddress> addresses = network.getInetAddresses();
                        if (addresses != null) {
                            while (addresses.hasMoreElements()) {
                                try {
                                    InetAddress address = addresses.nextElement();
                                    if (isValidHostAddress(address)) {
                                        localAddress = address;
                                        return address;
                                    }
                                } catch (Throwable e) {
                                    logger.warn("Failed to retriving network card ip address. cause:" + e.getMessage());
                                }
                            }
                        }
                    } catch (Throwable e) {
                        logger.warn("Failed to retriving network card ip address. cause:" + e.getMessage());
                    }
                }
            }
        } catch (Throwable e) {
            logger.warn("Failed to retriving network card ip address. cause:" + e.getMessage());
        }
        logger.error("Could not get local host ip address, will use 127.0.0.1 instead.");
        return localAddress;
    }

    public static InetAddress getMatchedAddress(String podIp) {
        InetAddress matchedHost = null;
        try {
            matchedHost = tryMatchFromLocalHost(podIp);
            if (matchedHost != null) {
                return matchedHost;
            }

            matchedHost = tryMatchFromAllNet(podIp, getNetInterface());
        } catch (Throwable e) {
            logger.error("Failed to matched pod ip to address. cause: " + e.getMessage());
        }
        return matchedHost;
    }

    public static InetAddress tryMatchFromLocalHost(String podIp) {
        try {
            InetAddress localHost = InetAddress.getLocalHost();
            if (isValidHostAddress(localHost) && podIp.equalsIgnoreCase(localHost.getHostAddress())) {
                return localHost;
            }
        } catch (Throwable e) {
            // just ignore this
        }
        return null;
    }

    public static InetAddress tryMatchFromAllNet(String podIp, Enumeration<NetworkInterface> interfaces) {
        if (interfaces != null) {
            while (interfaces.hasMoreElements()) {
                NetworkInterface network = interfaces.nextElement();
                Enumeration<InetAddress> addresses = network.getInetAddresses();
                if (addresses != null) {
                    while (addresses.hasMoreElements()) {
                        InetAddress address = addresses.nextElement();
                        if (isValidHostAddress(address) && podIp.equalsIgnoreCase(address.getHostAddress())) {
                            return address;
                        }
                    }
                }
            }
        }
        return null;
    }

    public static Enumeration<NetworkInterface> getNetInterface() throws SocketException {
        return NetworkInterface.getNetworkInterfaces();
    }
}
