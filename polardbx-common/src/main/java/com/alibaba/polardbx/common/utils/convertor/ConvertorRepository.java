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

package com.alibaba.polardbx.common.utils.convertor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ConvertorRepository {

    private static final String SEPERATOR = ":";
    private static String int_name = Integer.class.getName();
    private static String short_name = Short.class.getName();
    private static String long_name = Long.class.getName();
    private static String char_name = Character.class.getName();
    private static String void_name = Void.class.getName();
    private static String double_name = Double.class.getName();
    private static String float_name = Float.class.getName();
    private static String byte_name = Byte.class.getName();
    private static String bool_name = Boolean.class.getName();

    private Map<String, Convertor> convertors = new ConcurrentHashMap<String, Convertor>(10);

    public Convertor getConvertor(Class src, Class dest) {

        return convertors.get(mapperConvertorName(src, dest));
    }

    public Convertor getConvertor(String alias) {
        return convertors.get(alias);
    }

    public void registerConvertor(Class src, Class dest, Convertor convertor) {
        String key = mapperConvertorName(src, dest);

        if (convertor != null) {
            convertors.put(key, convertor);
        }
    }

    public void registerConvertor(String alias, Convertor convertor) {

        if (convertor != null) {
            convertors.put(alias, convertor);
        }
    }

    private String mapperConvertorName(Class src, Class dest) {
        String name1 = getName(src);
        String name2 = getName(dest);

        return name1 + SEPERATOR + name2;
    }

    private String getName(Class type) {
        if (type.isPrimitive()) {
            if (type == int.class) {
                return int_name;
            } else if (type == short.class) {
                return short_name;
            } else if (type == long.class) {
                return long_name;
            } else if (type == char.class) {
                return char_name;
            } else if (type == void.class) {
                return void_name;
            } else if (type == double.class) {
                return double_name;
            } else if (type == float.class) {
                return float_name;
            } else if (type == byte.class) {
                return byte_name;
            } else if (type == boolean.class) {
                return bool_name;
            }
        }

        return type.getName();
    }

}
