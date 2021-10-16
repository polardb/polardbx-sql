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

package com.alibaba.polardbx.common.utils.version;

import org.apache.commons.lang.StringUtils;

public class InstanceVersion {

    public static final String systemVersion = "instanceVersion";
    private static final String SERVER_ARGS = "serverArgs";
    private static final String VERSION_POSTFIX = "-PXC-" + Version.getVersion();
    private static final String regex = "\\d+\\.\\d+\\.\\d+";

    static volatile InstanceVersion instanceVersion = new InstanceVersion();
    private String VERSION_PREFIX_5 = "5.6.29";
    private String VERSION_PREFIX_8 = "8.0.3";
    private String VERSION_PREFIX = VERSION_PREFIX_5;

    public InstanceVersion() {
        initialVersion();
    }

    private void initialVersion() {
        try {
            final String instanceVersion = System.getProperty(systemVersion);
            if (!StringUtils.isEmpty(instanceVersion)) {
                setVersionPrefix(instanceVersion);
            } else {
                String serverArgs = System.getProperty(SERVER_ARGS);
                if (StringUtils.isNotEmpty(serverArgs)) {
                    String[] args = StringUtils.split(serverArgs, ';');
                    for (String arg : args) {
                        String[] config = StringUtils.split(arg, '=');
                        if (config.length == 2) {
                            if (config[0].equals(systemVersion)) {
                                setVersionPrefix(config[1]);
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {

        }
    }

    private void setVersionPrefix(String instanceVersion) {
        if (instanceVersion.equals("8")) {
            VERSION_PREFIX = VERSION_PREFIX_8;
        } else if (instanceVersion.equals("5")) {
            VERSION_PREFIX = VERSION_PREFIX_5;
        } else if (instanceVersion.matches(regex)) {
            VERSION_PREFIX = instanceVersion;
        }
    }

    public static void reloadVersion(String version) {
        if (StringUtils.isEmpty(version)) {
            return;
        }
        System.setProperty(systemVersion, version);
        InstanceVersion instanceVersion = new InstanceVersion();
        InstanceVersion.instanceVersion = instanceVersion;
    }

    public static String getVersion() {
        return instanceVersion.VERSION_PREFIX;
    }

    public static String getPostfixVersion() {
        return VERSION_POSTFIX;
    }

    public static String getFullVersion() {
        return instanceVersion.VERSION_PREFIX + VERSION_POSTFIX;
    }

}
