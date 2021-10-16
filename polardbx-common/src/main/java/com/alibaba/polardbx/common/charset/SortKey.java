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

import com.google.common.base.Preconditions;
import io.airlift.slice.SliceInput;

public class SortKey implements Comparable<SortKey> {
    private static final int UNSET_EFFECTIVE_LENGTH = -1;
    public final CharsetName charset;
    public final CollationName collation;

    public final byte keys[];

    public final SliceInput utf8Source;

    public int hashcode;

    public int length;

    public int effectiveLength;

    public SortKey(byte[] keys, int effectiveLength) {
        this(null, null, null, keys, effectiveLength);
    }

    public SortKey(SliceInput utf8Source, byte[] keys, int effectiveLength) {
        this(null, null, utf8Source, keys, effectiveLength);
    }

    public SortKey(CharsetName charset, CollationName collation, SliceInput utf8Source, byte[] keys) {
        this(charset, collation, utf8Source, keys, UNSET_EFFECTIVE_LENGTH);
    }

    public SortKey(CharsetName charset, CollationName collation, SliceInput utf8Source, byte[] keys,
                   int effectiveLength) {
        Preconditions.checkNotNull(keys);
        this.charset = charset;
        this.collation = collation;
        this.utf8Source = utf8Source;
        this.keys = keys;
        this.length = keys.length;
        this.hashcode = 0;
        this.effectiveLength = effectiveLength;
    }

    @Override
    public int compareTo(SortKey target) {
        int len = (effectiveLength == UNSET_EFFECTIVE_LENGTH || target.effectiveLength == UNSET_EFFECTIVE_LENGTH)

            ? Math.min(target.length, length)

            : Math.max(effectiveLength, target.effectiveLength);

        int i = 0;
        for (; i < len; i++) {
            if (keys[i] != target.keys[i]) {
                return Byte.toUnsignedInt(keys[i]) - Byte.toUnsignedInt(target.keys[i]);
            }
        }
        return length - target.length;
    }

    public boolean equals(SortKey target) {
        if (this == target) {
            return true;
        }
        if (target == null) {
            return false;
        }
        int i = 0;
        while (true) {
            if (keys[i] != target.keys[i]) {
                return false;
            }
            if (keys[i] == 0) {
                break;
            }
            i++;
        }
        return true;
    }

    @Override
    public int hashCode() {
        if (hashcode == 0) {
            int size = keys.length >> 1;
            StringBuilder key = new StringBuilder(size);
            for (int i = 0; i < length; i++) {
                if (i + 1 < length) {
                    key.append((char) ((keys[i] << 8) | (0xff & keys[i + 1])));
                    i++;
                } else {
                    key.append((char) (keys[i] << 8));
                }
            }
            hashcode = key.toString().hashCode();
        }
        return hashcode;
    }

}
