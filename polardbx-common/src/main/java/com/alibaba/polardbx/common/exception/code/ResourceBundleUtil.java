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

package com.alibaba.polardbx.common.exception.code;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import org.apache.commons.lang.StringUtils;

import java.text.MessageFormat;
import java.util.HashSet;
import java.util.MissingResourceException;
import java.util.ResourceBundle;
import java.util.Set;


public class ResourceBundleUtil {

    private static final Logger logger = LoggerFactory.getLogger(ResourceBundleUtil.class);
    private static final ResourceBundleUtil instance = new ResourceBundleUtil("res/ErrorCode");
    public static final String DEFAULT_PLACEHOLDER_PREFIX = "${";
    public static final String DEFAULT_PLACEHOLDER_SUFFIX = "}";
    public static final int SYSTEM_PROPERTIES_MODE_FALLBACK = 1;
    public static final int SYSTEM_PROPERTIES_MODE_OVERRIDE = 2;

    private String placeholderPrefix = DEFAULT_PLACEHOLDER_PREFIX;
    private String placeholderSuffix = DEFAULT_PLACEHOLDER_SUFFIX;
    private int systemPropertiesMode = 1;
    private boolean ignoreUnresolvablePlaceholders = false;
    private ResourceBundle bundle;

    public static ResourceBundleUtil getInstance() {
        return instance;
    }

    public ResourceBundleUtil(String bundleName) {
        this.bundle = ResourceBundle.getBundle(bundleName);
    }

    public String getMessage(String key, int code, String type, String... params) {

        if (key == null) {
            return null;
        }
        StringBuilder sb = new StringBuilder();

        sb.append(getString("ERR_PREFIX"))
            .append(" ")
            .append(getString(key))
            .append(" ")
            .append(getString("ERR_POSTFIX"));

        String msg = sb.toString();
        msg = parseStringValue(msg, new HashSet<String>());
        msg = StringUtils.replace(msg, "{code}", String.valueOf(code));
        msg = StringUtils.replace(msg, "{type}", String.valueOf(type));
        msg = StringUtils.replace(msg, "{key}", key);

        if (params == null || params.length == 0) {
            return msg;
        }

        if (StringUtils.isBlank(msg)) {

            return msg;
        }
        return MessageFormat.format(msg, (Object[]) params);
    }

    public String getMessage(String key, String... params) {

        if (key == null) {
            return null;
        }
        StringBuilder sb = new StringBuilder();

        sb.append(getString(key));

        String msg = parseStringValue(sb.toString(), new HashSet<String>());

        if (params == null || params.length == 0) {
            return msg;
        }

        if (StringUtils.isBlank(msg)) {

            return msg;
        }
        return MessageFormat.format(msg, (Object[]) params);
    }

    protected String parseStringValue(String strVal, Set<String> visitedPlaceholders) {
        StringBuffer buf = new StringBuffer(strVal);
        int startIndex = strVal.indexOf(placeholderPrefix);
        while (startIndex != -1) {
            int endIndex = findPlaceholderEndIndex(buf, startIndex);
            if (endIndex != -1) {
                String placeholder = buf.substring(startIndex + placeholderPrefix.length(), endIndex);
                if (!visitedPlaceholders.add(placeholder)) {
                    throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, "Circular placeholder reference '"
                        + placeholder + "' in bundle definitions");
                }

                placeholder = parseStringValue(placeholder, visitedPlaceholders);

                String propVal = resolvePlaceholder(placeholder, this.systemPropertiesMode);
                if (propVal != null) {
                    propVal = parseStringValue(propVal, visitedPlaceholders);
                    buf.replace(startIndex, endIndex + this.placeholderSuffix.length(), propVal);
                    if (logger.isTraceEnabled()) {
                        logger.trace("Resolved placeholder '" + placeholder + "'");
                    }
                    startIndex = buf.indexOf(this.placeholderPrefix, startIndex + propVal.length());
                } else if (this.ignoreUnresolvablePlaceholders) {

                    startIndex = buf.indexOf(this.placeholderPrefix, endIndex + this.placeholderSuffix.length());
                } else {
                    throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, "Could not resolve placeholder '"
                        + placeholder + "'");
                }
                visitedPlaceholders.remove(placeholder);
            } else {
                startIndex = -1;
            }
        }

        return buf.toString();
    }

    private int findPlaceholderEndIndex(CharSequence buf, int startIndex) {
        int index = startIndex + placeholderPrefix.length();
        int withinNestedPlaceholder = 0;
        while (index < buf.length()) {
            if (substringMatch(buf, index, placeholderSuffix)) {
                if (withinNestedPlaceholder > 0) {
                    withinNestedPlaceholder--;
                    index = index + placeholderSuffix.length();
                } else {
                    return index;
                }
            } else if (substringMatch(buf, index, placeholderPrefix)) {
                withinNestedPlaceholder++;
                index = index + placeholderPrefix.length();
            } else {
                index++;
            }
        }
        return -1;
    }

    private boolean substringMatch(CharSequence str, int index, CharSequence substring) {
        for (int j = 0; j < substring.length(); j++) {
            int i = index + j;
            if (i >= str.length() || str.charAt(i) != substring.charAt(j)) {
                return false;
            }
        }
        return true;
    }

    private String resolvePlaceholder(String placeholder, int systemPropertiesMode) {
        String propVal = null;
        if (systemPropertiesMode == SYSTEM_PROPERTIES_MODE_OVERRIDE) {
            propVal = resolveSystemProperty(placeholder);
        }
        if (propVal == null) {
            propVal = getString(placeholder);
        }
        if (propVal == null && systemPropertiesMode == SYSTEM_PROPERTIES_MODE_FALLBACK) {
            propVal = resolveSystemProperty(placeholder);
        }
        return propVal;
    }

    protected String getString(String key) {
        String value = null;
        if (bundle.containsKey(key)) {
            value = bundle.getString(key);
        }
        if (value == null) {
            return "";
        }
        return value;
    }

    private String resolveSystemProperty(String key) {
        try {
            String value = System.getProperty(key);
            if (value == null) {
                value = System.getenv(key);
            }
            return value;
        } catch (Throwable ex) {
            if (logger.isDebugEnabled()) {
                logger.debug("Could not access system property '" + key + "': " + ex);
            }
            return null;
        }
    }

}
