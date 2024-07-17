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

import com.google.common.collect.ImmutableSet;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class SystemPropertiesHelper {

    public static final String INST_ROLE = "instRole";
    private static volatile ImmutableSet<String> connectionProperties = null;

    public static void setPropertyValue(String propertyKey, Object propertyVal) {
        Properties properties = System.getProperties();
        properties.put(propertyKey, propertyVal);
    }

    public static Object getPropertyValue(String propertyKey) {
        Properties properties = System.getProperties();
        return properties.getProperty(propertyKey);
    }

    public static ImmutableSet<String> getConnectionProperties() {
        if (connectionProperties == null) {
            synchronized (SystemPropertiesHelper.class) {
                if (connectionProperties == null) {
                    List<String> propertyList = new ArrayList<>();
                    for (Field field : ConnectionProperties.class.getDeclaredFields()) {
                        try {
                            String key = field.get(ConnectionProperties.class).toString();
                            if (key != null) {
                                propertyList.add(key.toUpperCase());
                            }
                        } catch (IllegalAccessException ignored) {

                        }
                    }
                    connectionProperties = ImmutableSet.copyOf(propertyList);
                }
            }
        }
        return connectionProperties;
    }
}
