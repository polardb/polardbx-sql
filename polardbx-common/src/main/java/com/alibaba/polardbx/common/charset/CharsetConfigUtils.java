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

package com.alibaba.polardbx.common.charset;

import com.google.common.collect.ImmutableMap;
import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.common.charset.CollationName;
import org.yaml.snakeyaml.Yaml;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class CharsetConfigUtils {
    public static final String CONFIG_TO_UNICODE = "to_uni_";
    public static final String CONFIG_TO_UPPER = "to_upper_";
    public static final String CONFIG_TO_LOWER = "to_lower_";
    public static final String CONFIG_SORT_ORDER = "sort_order_";
    public static final String CONFIG_C_TYPE = "ctype_";
    public static final Map<String, int[]> COLLATION_CONFIGS;

    static {
        ImmutableMap.Builder mapBuilder = ImmutableMap.<String, int[]>builder();
        Yaml yaml = new Yaml();
        Map map = yaml.loadAs(CharsetConfigUtils.class.getResourceAsStream("simple_charset.conf.yml"), Map.class);
        map.forEach(
            (k, v) -> {
                if (k instanceof String && v instanceof List) {
                    List list = (List) v;
                    int[] array = new int[list.size()];
                    for (int i = 0; i < list.size(); i++) {
                        array[i] = (int) list.get(i);
                    }
                    mapBuilder.put(k, array);
                }
            }
        );
        COLLATION_CONFIGS = mapBuilder.build();
    }

    public static int[] sortOrderOf(CollationName collationName) {
        return Optional.ofNullable(collationName)
            .map(c -> CONFIG_SORT_ORDER + collationName.name().toLowerCase())
            .map(COLLATION_CONFIGS::get)
            .orElse(null);
    }

    public static int[] toLowerOf(CharsetName charsetName) {
        return Optional.ofNullable(charsetName)
            .map(c -> CONFIG_TO_LOWER + charsetName.getDefaultCollationName().name().toLowerCase())
            .map(COLLATION_CONFIGS::get)
            .orElse(null);
    }

    public static int[] toUpperOf(CharsetName charsetName) {
        return Optional.ofNullable(charsetName)
            .map(c -> CONFIG_TO_UPPER + charsetName.getDefaultCollationName().name().toLowerCase())
            .map(COLLATION_CONFIGS::get)
            .orElse(null);
    }

    public static int[] ctypeOf(CharsetName charsetName) {
        return Optional.ofNullable(charsetName)
            .map(c -> CONFIG_C_TYPE + charsetName.getDefaultCollationName().name().toLowerCase())
            .map(COLLATION_CONFIGS::get)
            .orElse(null);
    }

    public static int[] toUnicodeOf(CharsetName charsetName) {
        return Optional.ofNullable(charsetName)
            .map(c -> CONFIG_TO_UNICODE + charsetName.getDefaultCollationName().name().toLowerCase())
            .map(COLLATION_CONFIGS::get)
            .orElse(null);
    }
}
