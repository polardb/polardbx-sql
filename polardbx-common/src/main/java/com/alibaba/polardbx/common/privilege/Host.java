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

package com.alibaba.polardbx.common.privilege;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class Host implements Predicate<String> {

    public static final String DEFAULT_HOST = "%";

    public static final char FULL_MATCH_CHAR = '%';

    public static final char SINGLE_MATCH_CHAR = '_';

    private static final Cache<String, Pattern> PATTERN_CACHE = CacheBuilder.newBuilder()
        .concurrencyLevel(32)
        .initialCapacity(128)
        .maximumSize(1024)
        .build();

    private final String value;
    private final Pattern regPattern;

    public Host(String value) {
        boolean pass = verify(value);
        if (!pass) {
            throw new IllegalArgumentException("Illegal hostname: " + value);
        }

        this.value = value;
        try {
            this.regPattern = PATTERN_CACHE.get(value, () -> patternOf(value));
        } catch (ExecutionException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_SERVER, e, "Failed to create host matcher!");
        }
    }

    static boolean isIPV4Char(char ch) {
        return (ch >= '0' && ch <= '9') || ch == '.' || ch == '_' || ch == '%';
    }


    static boolean isIPV6Char(char ch) {
        return (ch >= '0' && ch <= '9') || (ch >= 'A' && ch <= 'F') || (ch >= 'a' && ch <= 'f') || ch == ':'
            || ch == '_' || ch == '%';
    }

    public static boolean verify(String host) {
        if (host == null || host.isEmpty()) {
            return false;
        }

        for (int i = 0; i < host.length(); i++) {
            char ch = host.charAt(i);
            if (!isIPV4Char(ch) && !isIPV6Char(ch)) {
                return false;
            }
        }
        return true;
    }

    private static Pattern patternOf(String value) {
        Preconditions.checkNotNull(value, "Value can't be null!");
        StringBuilder sb = new StringBuilder(value.length());

        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c == '.') {
                sb.append("#");
            } else if (c == SINGLE_MATCH_CHAR) {
                sb.append(".");
            } else if (c == FULL_MATCH_CHAR) {
                sb.append(".*");
            } else {
                sb.append(c);
            }
        }

        return Pattern.compile(sb.toString());
    }

    public static boolean containsWildcardChars(String host) {
        for (char ch : host.toCharArray()) {
            if (ch == FULL_MATCH_CHAR || ch == SINGLE_MATCH_CHAR) {
                return true;
            }
        }

        return false;
    }

    public String getValue() {
        return value;
    }

    public boolean matches(String host) {
        if (!verify(host)) {
            return false;
        }
        if (Objects.equals(host, value)) {
            return true;
        }
        host = host.replace('.', '#');
        Matcher m = regPattern.matcher(host);
        return m.matches();
    }

    @Override
    public boolean test(String s) {
        return matches(s);
    }
}
