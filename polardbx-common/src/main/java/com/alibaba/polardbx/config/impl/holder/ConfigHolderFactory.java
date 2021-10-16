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

package com.alibaba.polardbx.config.impl.holder;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ConfigHolderFactory {

    private static Map<String, ConfigDataHolder> holderMap = new ConcurrentHashMap<String, ConfigDataHolder>();

    public static ConfigDataHolder getConfigDataHolder(String appName) {
        return holderMap.get(appName);
    }

    public static void addConfigDataHolder(String appName, ConfigDataHolder configDataHolder) {
        holderMap.put(appName, configDataHolder);
    }

    public static void removeConfigHoder(String appName) {
        holderMap.remove(appName);
    }

    public static boolean isInit(String appName) {
        return appName != null && holderMap.containsKey(appName);
    }

}
