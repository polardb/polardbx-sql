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

package com.alibaba.polardbx.config;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.ToStringBuilder;

import com.alibaba.polardbx.server.util.StringUtil;
import com.googlecode.ipv6.IPv6Address;
import com.googlecode.ipv6.IPv6AddressRange;
import com.googlecode.ipv6.IPv6Network;
import com.taobao.tddl.common.utils.TddlToStringStyle;

/**
 * 隔离区配置定义
 * <p>
 * <pre>
 * ip定义：127.0.0.1,127.0.0.*,127.0.0.0/24,127.0.0.1-127.0.0.10
 * </pre>
 *
 * @author haiqing.zhuhq 2012-4-17
 */
public final class QuarantineConfig {

    /**
     * Format is 10.20.0.1-10.20.255.255
     */
    static class NetScope {

        public long startAddr;
        public long endAddr;

        public NetScope(String inputScope) throws IllegalArgumentException {
            if (StringUtil.isEmpty(inputScope)) {
                throw new IllegalArgumentException("Input scope is empty!");
            }

            inputScope = inputScope.trim();

            int pos;
            if (-1 != (pos = inputScope.indexOf("-"))) {
                String start = inputScope.substring(0, pos);
                String end = inputScope.substring(pos + 1);
                end = end.replace("-", "");
                startAddr = ip2long(start);
                endAddr = ip2long(end);
            } else {
                throw new IllegalArgumentException("input: " + inputScope + " is not supported!");
            }
        }

        public boolean isInScope(long ip) {
            if ((ip <= endAddr) == (ip >= startAddr)) {
                return true;
            }
            return false;
        }
    }

    /**
     * Netmask may have several presentation 10.20.*.* 10.20.0.0/24 Should first
     * convert input ip to long then check if it's in the scope, since we use
     * simple string to store the Netmaks, it may be duplicated for one mask
     * defination. Do not need to store original mask string, since it will
     * always clear all content when set a new value.
     */
    static class Netmask {

        public long startAddr;
        public int submask;

        public Netmask(String inputmask) throws IllegalArgumentException {
            if (StringUtil.isEmpty(inputmask)) {
                throw new IllegalArgumentException("Input maks is empty!");
            }

            inputmask = inputmask.trim();

            if (-1 != inputmask.indexOf("/")) {
                // format is 10.20.0.0/24
                int type = Integer.parseInt(inputmask.replaceAll(".*/", ""));
                int mask = 0xFFFFFFFF << (32 - type);
                String cidrIp = inputmask.replaceAll("/.*", "");
                startAddr = ip2long(cidrIp);
                submask = mask;
            } else if (-1 != inputmask.indexOf("*")) {
                // format is 10.20.*.*
                submask = 0xFFFFFFFF;
                String[] fourPlay = inputmask.split("\\.");
                for (int i = 0; i < fourPlay.length; i++) {
                    if (!fourPlay[i].equals("*")) {
                        startAddr += addressItem(inputmask, fourPlay[i]) << ((3 - i) * 8);
                    } else {
                        submask &= ~(0xFF << (3 - i) * 8);
                    }
                }
            } else {
                throw new IllegalArgumentException("input: " + inputmask + " is not supported!");
            }

        }

        public boolean isInScope(long ip) {
            if ((ip & submask) == (startAddr & submask)) {
                return true;
            }
            return false;
        }
    }

    class UserIpDefines implements IUserHostDefination {

        Set<String> hosts;
        List<Netmask> netmasks;
        List<NetScope> netscopes;

        List<IPv6Address> iPv6Hosts;
        List<IPv6AddressRange> iPv6AddressRangeList;
        List<IPv6Network> iPv6Networks;

        public UserIpDefines(Set<String> hosts, List<Netmask> netmasks, List<NetScope> netscopes,
                             List<IPv6Address> iPv6Hosts, List<IPv6AddressRange> iPv6AddressRangeList,
                             List<IPv6Network> iPv6Networks) {
            this.hosts = hosts;
            this.netmasks = netmasks;
            this.netscopes = netscopes;
            this.iPv6Hosts = iPv6Hosts;
            this.iPv6AddressRangeList = iPv6AddressRangeList;
            this.iPv6Networks = iPv6Networks;
        }

        public boolean isEmpty() {
            return hosts.isEmpty()
                && netmasks.isEmpty()
                && netscopes.isEmpty()
                && iPv6Hosts.isEmpty()
                && iPv6AddressRangeList.isEmpty()
                && iPv6Networks.isEmpty();
        }

        /**
         * ordered by host --> mask --> scope
         */
        public boolean containsOf(String host) {
            if (host.indexOf("%") != -1) { // for the case 2401:b180:1000:2d:f111:8e04:e9c9:257d%123
                host = host.split("%")[0];
            }
            if (isIpv4(host)) {
                if (hosts.contains(host)) {
                    return true;
                }
                long ip = ip2long(host);
                return containsOf(ip);
            } else if (isIpv6(host)) {
                IPv6Address iPv6Address = IPv6Address.fromString(host);
                return containsOf(iPv6Address);
            } else {
                return false;
            }
        }

        public boolean containsOf(long host) {
            for (int i = 0; i < netmasks.size(); i++) {
                if (netmasks.get(i).isInScope(host)) {
                    return true;
                }
            }

            for (int i = 0; i < netscopes.size(); i++) {
                if (netscopes.get(i).isInScope(host)) {
                    return true;
                }
            }

            return false;
        }

        public boolean containsOf(IPv6Address iPv6Address) {
            for (IPv6Address address : iPv6Hosts) {
                if (address.equals(iPv6Address)) {
                    return true;
                }
            }

            for (IPv6Network network : iPv6Networks) {
                if (network.contains(iPv6Address)) {
                    return true;
                }
            }

            for (IPv6AddressRange addressRange : iPv6AddressRangeList) {
                if (addressRange.contains(iPv6Address)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public String toString() {
            return ToStringBuilder.reflectionToString(this, TddlToStringStyle.DEFAULT_STYLE);
        }

    }

    private final Map<String /* user/appName */, IUserHostDefination /* userIpDefines */> hosts;
    /**
     * black list for cluster , maybe null
     */
    private IUserHostDefination clusterBl = null;
    /**
     * white list for cluster , maybe null
     */
    private IUserHostDefination clusterWl = null;
    /**
     * trusted ip for internal usage
     */
    private IUserHostDefination trustedips = null;

    /**
     * truster ip for cluster nodes
     */
    private IUserHostDefination clusterips = null;

    public QuarantineConfig() {
        hosts = new HashMap<String, IUserHostDefination>();
    }

    public Map<String, IUserHostDefination> getHosts() {
        return hosts;
    }

    public void cleanHostByApp(String app) {
        if (hosts.containsKey(app)) {
            hosts.remove(app);
        }
    }

    /**
     * No need to determine if .xxx. should in [0,255] because the request in ip
     * is generated by system, even if the definition is not so accurate, the
     * purpose should always be valid.
     */
    static long ip2long(String ip) {
        String[] ips = ip.split("\\.");
        long result = 0;
        for (int i = 0; i < ips.length; i++) {
            result += addressItem(ip, ips[i]) << ((3 - i) * 8);
        }

        return result;
    }

    static int addressItem(String orig, String item) throws IllegalArgumentException {
        int result = Integer.parseInt(item);

        if (result < 0 || result > 255) {
            throw new IllegalArgumentException("Address: " + orig + " is not valid");
        }

        return result;
    }

    /**
     * If we met format or addressItem invalid exception, then it will be thrown
     * and keep the original unchanged.
     */
    private IUserHostDefination parseHostDefinations(String config) {
        HashSet<String> hostset = new HashSet<String>();
        ArrayList<NetScope> netscopes = new ArrayList<NetScope>();
        ArrayList<Netmask> netmasks = new ArrayList<Netmask>();
        ArrayList<IPv6Address> iPv6Hosts = new ArrayList<IPv6Address>();
        List<IPv6AddressRange> iPv6AddressRangeList = new ArrayList<>();
        List<IPv6Network> iPv6Networks = new ArrayList<>();
        if (StringUtils.isEmpty(config)) {
            return new UserIpDefines(hostset, netmasks, netscopes, iPv6Hosts, iPv6AddressRangeList, iPv6Networks);
        }

        String[] quarantinesByHost = StringUtil.split(config, ',', true);
        for (String host : quarantinesByHost) {
            // 0.0.0.0/0 matches all hosts
            if (StringUtils.equals(host, "0.0.0.0/0")) {
                host = "*.*.*.*";
            }

            if (host.indexOf("%") != -1) { // for the case 2401:b180:1000:2d:f111:8e04:e9c9:257d%123
                host = host.split("%")[0];
            }
            if (-1 != host.indexOf("/") || -1 != host.indexOf("*")) {
                if (isIpv6Network(host)) {
                    iPv6Networks.add(IPv6Network.fromString(host));
                } else {
                    netmasks.add(new Netmask(host));
                }
            } else if (-1 != host.indexOf("-")) {
                final int i = host.indexOf("-");
                String start = host.substring(0, i);
                String end = host.substring(i + 1);
                end = end.replace("-", "");
                if (isIpv4(start) && isIpv4(end)) {
                    netscopes.add(new NetScope(host));
                } else if (isIpv6(start) && isIpv6(end)) {
                    iPv6AddressRangeList.add(
                        IPv6AddressRange.fromFirstAndLast(IPv6Address.fromString(start), IPv6Address.fromString(end)));
                }

            } else {
                if (isIpv4(host)) {
                    hostset.add(host);
                } else if (isIpv6(host)) {
                    iPv6Hosts.add(IPv6Address.fromString(host));
                }
                /**
                 * Put to quick hash set, no need to keep the sequence, if hosts
                 * is empty, then hostset will be empty as well;
                 */
            }
        }

        return new UserIpDefines(hostset, netmasks, netscopes, iPv6Hosts, iPv6AddressRangeList, iPv6Networks);
    }

    private boolean isIpv6Network(String text) {
        try {
            IPv6Network.fromString(text);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    private boolean isIpv6(String text) {
        try {
            IPv6Address.fromString(text);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }

    private boolean isIpv4(String text) {
        if (StringUtils.isEmpty(text)) {
            return false;
        }

        String regex = "^(1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|[1-9])\\."
            + "(1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|\\d)\\."
            + "(1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|\\d)\\."
            + "(1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|\\d)$";
        if (text.matches(regex)) {
            return true;
        } else {
            return false;
        }
    }

    public void resetTrustedIps(String config) {
        this.trustedips = parseHostDefinations(config);
    }

    public void resetClusterIps(Set<String> trustIps) {
        Set<String> iPv4Host = new HashSet<>();
        ArrayList<NetScope> netscopes = new ArrayList<NetScope>();
        ArrayList<Netmask> netmasks = new ArrayList<Netmask>();
        ArrayList<IPv6Address> iPv6Hosts = new ArrayList<IPv6Address>();
        List<IPv6AddressRange> iPv6AddressRangeList = new ArrayList<>();
        List<IPv6Network> iPv6Networks = new ArrayList<>();
        for (String ip : trustIps) {
            if (isIpv4(ip)) {
                iPv4Host.add(ip);
            } else if (isIpv6(ip)) {
                iPv6Hosts.add(IPv6Address.fromString(ip));
            }
        }
        this.clusterips =
            new UserIpDefines(iPv4Host, netmasks, netscopes, iPv6Hosts, iPv6AddressRangeList, iPv6Networks);
    }

    public void resetBlackList(String config) {
        this.clusterBl = parseHostDefinations(config);
    }

    public void resetWhiteList(String config) {
        this.clusterWl = parseHostDefinations(config);
    }

    public void resetHosts(String app, String config) {
        this.hosts.put(app, parseHostDefinations(config));
    }

    public IUserHostDefination getClusterBlacklist() {
        return clusterBl;
    }

    public IUserHostDefination getClusterWhitelist() {
        return clusterWl;
    }

    public IUserHostDefination getTrustedIps() {
        return trustedips;
    }

    public IUserHostDefination getClusterIps() {
        return clusterips;
    }

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, TddlToStringStyle.DEFAULT_STYLE);
    }

}
