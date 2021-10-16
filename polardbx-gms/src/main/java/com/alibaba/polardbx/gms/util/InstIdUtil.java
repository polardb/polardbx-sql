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

package com.alibaba.polardbx.gms.util;

/**
 * @author chenghui.lch
 */
public class InstIdUtil {

    public static String getInstId() {
        // If Server startup by gms,
        // instanceId must be put into System.properties
        String instId = System.getProperty("instanceId");
        return instId;
    }

    public static String getMasterInstId() {
        // If Server startup by gms,
        // and server is read-only inst, then masterInstanceId must be not null
        String instId = System.getProperty("masterInstanceId");
        return instId;
    }

    public static String getInstIdFromStorageInfoDataId(String storageInfoDataId) {
        int prefixLen = "polardbx.storage.info.".length();
        String instId = storageInfoDataId.substring(prefixLen);
        return instId;
    }
}
