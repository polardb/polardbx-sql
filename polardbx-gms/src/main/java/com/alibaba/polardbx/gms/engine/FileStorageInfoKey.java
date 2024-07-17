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

package com.alibaba.polardbx.gms.engine;

import com.alibaba.polardbx.common.utils.TStringUtil;

public enum FileStorageInfoKey {
    ENGINE,
    ENDPOINT,

    FILE_URI,
    FILE_SYSTEM_CONF,

    ACCESS_KEY_ID,
    ACCESS_KEY_SECRET,

    PRIORITY,

    REGION_ID,
    AVAILABLE_ZONE_ID,

    CACHE_POLICY,
    DELETE_POLICY,
    STATUS,

    ENDPOINT_ORDINAL,

    // DefaultEndpointsProtocol=https;AccountName=openpolardbx;AccountKey=xxxxxxxxxx+x/xxxxxxxxx/xxxx+xx==;EndpointSuffix=core.windows.net
    AZURE_CONNECTION_STRING,
    AZURE_CONTAINER_NAME;

    public enum AzureConnectionStringKey {
        DefaultEndpointsProtocol,
        AccountName,
        AccountKey,
        EndpointSuffix;

        public static AzureConnectionStringKey of(String key) {
            if (TStringUtil.isEmpty(key)) {
                return null;
            }
            for (AzureConnectionStringKey record : values()) {
                if (record.name().equalsIgnoreCase(key)) {
                    return record;
                }
            }
            return null;
        }
    }

    public static FileStorageInfoKey of(String key) {
        if (TStringUtil.isEmpty(key)) {
            return null;
        }
        for (FileStorageInfoKey record : values()) {
            if (record.name().equalsIgnoreCase(key)) {
                return record;
            }
        }
        return null;
    }
}
