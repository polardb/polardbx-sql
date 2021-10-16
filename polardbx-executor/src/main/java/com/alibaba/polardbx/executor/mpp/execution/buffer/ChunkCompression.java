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

package com.alibaba.polardbx.executor.mpp.execution.buffer;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;

public enum ChunkCompression {

    UNCOMPRESSED((byte) 0),
    COMPRESSED((byte) 1);

    private final byte marker;

    ChunkCompression(byte marker) {
        this.marker = marker;
    }

    public byte getMarker() {
        return marker;
    }

    public static ChunkCompression lookupCodecFromMarker(byte marker) {
        if (marker != UNCOMPRESSED.getMarker() && marker != COMPRESSED.getMarker()) {
            throw new TddlRuntimeException(ErrorCode.ERR_CORRUPT_PAGE, "Page marker did not contain expected value");
        }
        return UNCOMPRESSED.getMarker() == marker ? UNCOMPRESSED : COMPRESSED;
    }
}
