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

package com.alibaba.polardbx.common.properties;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;

public class SystemPropertiesHelper {

    public static final String INST_ROLE = "instRole";
    public static Set<String> connectionProperties = null;

    public static void setPropertyValue(String propertyKey, Object propertyVal) {
        Properties properties = System.getProperties();
        properties.put(propertyKey, propertyVal);
    }

    public static Object getPropertyValue(String propertyKey) {
        Properties properties = System.getProperties();
        return properties.getProperty(propertyKey);
    }

    public static Set<String> getConnectionProperties() {
        if (connectionProperties == null) {
            connectionProperties = new HashSet<>();
            for (Field field : ConnectionProperties.class.getDeclaredFields()) {
                try {
                    String key = field.get(ConnectionProperties.class).toString();
                    connectionProperties.add(key.toUpperCase(Locale.ROOT));
                } catch (IllegalAccessException ignored) {
                    
                }
            }
        }
        return connectionProperties;
    }
}
