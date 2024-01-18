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

import com.alibaba.polardbx.common.utils.TStringUtil;

public enum OSSFileType {
    OTHER("", "", ""),
    TABLE_FORMAT("format", "%s_%s_%s.%s", "/tmp/%s_%s_%s.%s"),
    TABLE_FILE("orc", "%s_%s_%s.%s", "/tmp/%s_%s_%s.%s"),
    TABLE_META("bf", "%s_%s_%s_%s_%s.%s", "/tmp/%s_%s_%s_%s_%s.%s"),
    EXPORT_ORC_FILE("orc", "%s_%s.%s", "../spill/temp/%s");

    String suffix;
    String remotePathFormat;
    String localPathFormat;

    OSSFileType(String suffix, String remotePathFormat, String localPathFormat) {
        this.suffix = suffix;
        this.remotePathFormat = remotePathFormat;
        this.localPathFormat = localPathFormat;
    }

    public String getSuffix() {
        return suffix;
    }

    public String getRemotePathFormat() {
        return remotePathFormat;
    }

    public String getLocalPathFormat() {
        return localPathFormat;
    }

    public static OSSFileType of(String s) {
        OSSFileType res;
        if (TStringUtil.isEmpty(s) || (res = valueOf(s)) == null) {
            return OTHER;
        }
        return res;
    }
}
