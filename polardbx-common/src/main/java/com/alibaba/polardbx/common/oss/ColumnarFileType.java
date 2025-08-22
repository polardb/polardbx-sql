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

package com.alibaba.polardbx.common.oss;

import java.util.HashMap;
import java.util.Map;

public enum ColumnarFileType {
    ORC, CSV, DEL, SET,
    /**
     * PK IDX log and meta(one record per partition, used as meta lock)
     */
    PK_IDX_LOG,
    PK_IDX_LOG_META,
    PK_IDX_SNAPSHOT,
    PK_IDX_LOCK,
    PK_IDX_SST,
    PK_IDX_BF;

    static Map<String, ColumnarFileType> COLUMAR_FILE_TYPE_MAP = new HashMap<>();

    static {
        for (ColumnarFileType columnarFileType : ColumnarFileType.values()) {
            COLUMAR_FILE_TYPE_MAP.put(columnarFileType.name(), columnarFileType);
            COLUMAR_FILE_TYPE_MAP.put(columnarFileType.name().toLowerCase(), columnarFileType);
        }
    }

    public static ColumnarFileType of(String fileType) {
        if (fileType == null) {
            return null;
        }

        ColumnarFileType result;
        if ((result = COLUMAR_FILE_TYPE_MAP.get(fileType)) != null) {
            return result;
        }

        try {
            return valueOf(fileType);
        } catch (Throwable throwable) {
            return null;
        }
    }

    public boolean isDeltaFile() {
        return this == DEL || this == CSV;
    }
}
