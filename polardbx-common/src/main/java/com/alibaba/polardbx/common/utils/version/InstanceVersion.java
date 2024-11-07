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

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import org.apache.commons.lang.StringUtils;

public class InstanceVersion {

    public static final String systemVersion = "instanceVersion";
    private static final Logger logger = LoggerFactory.getLogger(InstanceVersion.class);
    private static final String SERVER_ARGS = "serverArgs";
    private static final String VERSION_POSTFIX = "-PXC-" + Version.getVersion();
    private static final String regex = "\\d+\\.\\d+\\.\\d+";
    private static final String VERSION_PREFIX_56 = "5.6.29";
    private static final String VERSION_PREFIX_57 = "5.7.25";
    private static final String VERSION_PREFIX_8 = "8.0.32";
    private static final String DEFAULT_VERSION_PREFIX = VERSION_PREFIX_56;
    static volatile InstanceVersion instanceVersion = new InstanceVersion();
    /**
     * 该变量仅用作内核判断
     * 不影响版本号前缀
     */
    private static boolean MYSQL80 = false;
    private String VERSION_PREFIX = DEFAULT_VERSION_PREFIX;

    public InstanceVersion() {
        initialVersion();
    }

    public static String parseVersionPrefix(String instanceVersion) throws IllegalVersionException {
        if (instanceVersion.equals("5") || instanceVersion.equals("56")
            || instanceVersion.equals("5.6")) {
            return VERSION_PREFIX_56;
        } else if (instanceVersion.equals("57") || instanceVersion.equals("5.7")) {
            return VERSION_PREFIX_57;
        } else if (instanceVersion.equals("8") || instanceVersion.equals("80")
            || instanceVersion.equals("8.0")) {
            return VERSION_PREFIX_8;
        } else if (instanceVersion.equalsIgnoreCase("default")) {
            return DEFAULT_VERSION_PREFIX;
        } else if (instanceVersion.matches(regex)) {
            return instanceVersion;
        }
        throw new IllegalVersionException();
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

    public static boolean isMYSQL80() {
        return MYSQL80;
    }

    public static void setMYSQL80(boolean MYSQL80) {
        InstanceVersion.MYSQL80 = MYSQL80;
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
            logger.warn("failed to init InstanceVersion", e);
        }
    }

    private void setVersionPrefix(String instanceVersion) {
        try {
            VERSION_PREFIX = parseVersionPrefix(instanceVersion);
        } catch (IllegalVersionException e) {
            logger.error("Illegal version prefix: " + instanceVersion);
            VERSION_PREFIX = DEFAULT_VERSION_PREFIX;
        }
    }

    public static class IllegalVersionException extends Exception {

    }
}
