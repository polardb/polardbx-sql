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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.SystemUtils;

import java.net.URL;
import java.security.CodeSource;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;

public final class Version {

    private Version() {
    }

    // TODO get product version from config property
    public static final String PRODUCT_VERSION = "2.4.2";

    private static final Logger logger = LoggerFactory.getLogger(Version.class);
    private static final Package myPackage = VersionAnnotation.class.getPackage();
    private static final VersionAnnotation va = myPackage.getAnnotation(VersionAnnotation.class);
    private static final String DEFAULT_VERSION = "5.x";
    private static final String VERSION = getVersion(Version.class, DEFAULT_VERSION);

    private static volatile Boolean versionChecked = false;

    public static void checkVersion() {
        if (versionChecked) {
            return;
        }

        synchronized (versionChecked) {
            if (versionChecked) {
                return;
            }
            try {
                // 可能有误判，跳过检测
                boolean versionCheck = true;
                if (System.getProperty("tddl.version.check") != null) {
                    versionCheck = Boolean.valueOf(System.getProperty("tddl.version.check"));

                }

                // 检查下sequence包
                Version.checkDuplicate("com/taobao/tddl/client/sequence/impl/GroupSequenceDao.class",
                    true && versionCheck);
                validVersion("polardbx-executor",
                    "com/alibaba/polardbx/sequence/impl/GroupSequenceDao.class",
                    VERSION,
                    false);
                Version.checkDuplicate("com/alibaba/polardbx/sequence/impl/SimpleSequenceDao.class",
                    true && versionCheck);
                validVersion("polardbx-executor",
                    "com/alibaba/polardbx/sequence/impl/SimpleSequenceDao.class",
                    VERSION,
                    false);
                // 检查下经常性冲突的两个包
                Version.checkDuplicate("com/alibaba/druid/pool/DruidDataSource.class", true && versionCheck);
                validVersion("druid", "com/alibaba/druid/pool/DruidDataSource.class", "1.0.15", true && versionCheck);
                Version.checkDuplicate("com/taobao/diamond/client/Diamond.class", true && versionCheck);
                validVersion("diamond-client", "com/taobao/diamond/client/Diamond.class", "3.6.8",
                    true && versionCheck);
                Version.checkDuplicate("com/google/common/collect/MapMaker.class", false);
                validVersion("guava", "com/google/common/collect/MapMaker.class", "15.0", false);
                // 检查下mysql driver
                Version.checkDuplicate("com/mysql/jdbc/Driver.class", true && versionCheck);
                validVersion("mysql-connector-java", "com/mysql/jdbc/Driver.class", "5.1.26", true && versionCheck);
                // 检查是否存在多logger
                Version.checkDuplicate("org/slf4j/impl/StaticLoggerBinder.class", false);
            } finally {
                versionChecked = true;
            }
        }
    }

    public static String getVersion() {
        return VERSION;
    }

    /**
     * Returns the detail version info
     */
    public static String getBuildVersion() {
        Package myPackage = VersionAnnotation.class.getPackage();
        VersionAnnotation va = myPackage.getAnnotation(VersionAnnotation.class);
        StringBuilder buf = new StringBuilder();
        buf.append(SystemUtils.LINE_SEPARATOR);
        buf.append("[TDDL Version Info]").append(SystemUtils.LINE_SEPARATOR);
        buf.append("[version ]").append(VERSION).append(SystemUtils.LINE_SEPARATOR);
        buf.append("[revision]").append(va != null ? va.revision() : "Unknown").append(SystemUtils.LINE_SEPARATOR);
        buf.append("[date    ]").append(va != null ? va.date() : "Unknown").append(SystemUtils.LINE_SEPARATOR);
        buf.append("[url     ]").append(va != null ? va.url() : "Unknown").append(SystemUtils.LINE_SEPARATOR);
        buf.append("[branch  ]").append(va != null ? va.branch() : "Unknown").append(SystemUtils.LINE_SEPARATOR);
        buf.append("[checksum]").append(va != null ? va.srcChecksum() : "Unknown").append(SystemUtils.LINE_SEPARATOR);
        return buf.toString();
    }

    public static String getVersion(Class<?> cls, String defaultVersion) {
        if (va != null) {
            // 如果version不是空字符,那就直接以这个为准
            if (StringUtils.isNotEmpty(va.version()) && !defaultVersion.equals(va.version())) {
                return va.version();
            }
        }

        try {
            // 首先查找MANIFEST.MF规范中的版本号
            String version = cls.getPackage().getImplementationVersion();
            if (version == null || version.length() == 0) {
                version = cls.getPackage().getSpecificationVersion();
            }
            if (version == null || version.length() == 0) {
                // 如果规范中没有版本号，基于jar包名获取版本号
                CodeSource codeSource = cls.getProtectionDomain().getCodeSource();
                if (codeSource == null) {
                    logger.info("No codeSource for class " + cls.getName() + " when getVersion, use default version "
                        + defaultVersion);
                } else {
                    String file = codeSource.getLocation().getFile();
                    version = getVersionByPath(file);
                }
            }

            if (checkVersionNecessary(version)) {
                // 返回版本号，如果为空返回缺省版本号
                return version == null || version.length() == 0 ? defaultVersion : version;
            } else {
                return defaultVersion;
            }
        } catch (Throwable e) { // 防御性容错
            // 忽略异常，返回缺省版本号
            logger.error("return default version, ignore exception " + e.getMessage(), e);
            return defaultVersion;
        }
    }

    /**
     * 检查下对应class path的版本，是否>minVersion
     */
    public static boolean validVersion(String name, String path, String minVersion, boolean failOnError) {
        try {
            if (minVersion == null) {
                return true;
            }

            Long minv = convertVersion(minVersion);
            Enumeration<URL> urls = Version.class.getClassLoader().getResources(path);
            while (urls.hasMoreElements()) {
                URL url = urls.nextElement();
                if (url != null) {
                    String file = url.getFile();
                    if (file != null && file.length() > 0) {
                        String version = getVersionByPath(file);
                        if (checkVersionNecessary(version)) {
                            Long ver = convertVersion(version);
                            if (ver < minv) {
                                try {
                                    throw new TddlRuntimeException(ErrorCode.ERR_VERSION_TOO_LOW,
                                        name,
                                        version,
                                        minVersion);
                                } catch (TddlRuntimeException e) {
                                    if (!failOnError) {
                                        logger.error(e.getMessage());
                                    } else {
                                        throw e;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } catch (TddlRuntimeException e) {
            throw e;
        } catch (Throwable e) { // 防御性容错
            logger.error(e.getMessage(), e);
        }

        return true;
    }

    public static boolean checkDuplicate(Class<?> cls, boolean failOnError) {
        return checkDuplicate(cls.getName().replace('.', '/') + ".class", failOnError);
    }

    public static boolean checkDuplicate(Class<?> cls) {
        return checkDuplicate(cls, false);
    }

    public static boolean checkDuplicate(String path, boolean failOnError) {
        try {
            // 在ClassPath搜文件
            Enumeration<URL> urls = Version.class.getClassLoader().getResources(path);
            Set<String> files = new HashSet<String>();
            while (urls.hasMoreElements()) {
                URL url = urls.nextElement();
                if (url != null) {
                    String file = url.getFile();
                    if (file != null && file.length() > 0) {
                        files.add(file);
                    }
                }
            }

            // 如果有多个，就表示重复
            if (files.size() > 1) {
                try {
                    throw new TddlRuntimeException(ErrorCode.ERR_DUPLICATED_CLASS,
                        path,
                        String.valueOf(files.size()),
                        String.valueOf(files));
                } catch (TddlRuntimeException e) {
                    if (!failOnError) {
                        logger.error(e.getMessage());
                    } else {
                        throw e;
                    }
                }

                return true;
            }
        } catch (TddlRuntimeException e) {
            throw e;
        } catch (Throwable e) { // 防御性容错
            logger.error(e.getMessage(), e);
        }

        return false;
    }

    /**
     * 根据jar包的路径，找到对应的版本号
     */
    public static String getVersionByPath(String file) {
        if (file != null && file.length() > 0 && StringUtils.contains(file, ".jar")) {
            int index = StringUtils.lastIndexOf(file, ".jar");
            file = file.substring(0, index);
            int i = file.lastIndexOf('/');
            if (i >= 0) {
                file = file.substring(i + 1);
            }
            i = file.indexOf("-");
            if (i >= 0) {
                file = file.substring(i + 1);
            }
            while (file.length() > 0 && !Character.isDigit(file.charAt(0))) {
                i = file.indexOf("-");
                if (i >= 0) {
                    file = file.substring(i + 1);
                } else {
                    break;
                }
            }
            return file;
        } else {
            return null;
        }
    }

    public static Long convertVersion(String version) {
        String parts[] = StringUtils.split(version, '.');
        Long result = 0l;
        int i = 1;
        int size = parts.length > 4 ? parts.length : 4;
        for (String part : parts) {
            if (StringUtils.isNumeric(part)) {
                result += Long.valueOf(part) * Double.valueOf(Math.pow(100, (size - i))).longValue();
            } else {
                String subParts[] = StringUtils.split(part, '-');
                if (StringUtils.isNumeric(subParts[0])) {
                    result += Long.valueOf(subParts[0]) * Double.valueOf(Math.pow(100, (size - i))).longValue();
                }
            }

            i++;
        }

        return result;
    }

    private static boolean checkVersionNecessary(String versionStr) {
        return !(versionStr == null || StringUtils.contains(versionStr, "with-dependencies")
            || StringUtils.contains(versionStr, "storm") || StringUtils.contains(versionStr, "odps"));
    }
}
