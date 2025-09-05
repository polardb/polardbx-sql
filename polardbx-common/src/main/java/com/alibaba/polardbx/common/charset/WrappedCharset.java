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

import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;

public class WrappedCharset extends Charset {
    private Charset originalCharset;

    protected WrappedCharset(String canonicalName, String charsetName) {
        super(canonicalName, new String[] {canonicalName});
        originalCharset = Charset.forName(charsetName);
    }

    protected static Charset of(CharsetName charsetName) {
        return new WrappedCharset(charsetName.getJavaCharset(), charsetName.getOriginalCharset());
    }

    @Override
    public boolean contains(Charset cs) {
        return originalCharset.contains(cs);
    }

    @Override
    public CharsetDecoder newDecoder() {
        return originalCharset.newDecoder();
    }

    @Override
    public CharsetEncoder newEncoder() {
        return originalCharset.newEncoder();
    }

    public Charset getOriginalCharset() {
        return originalCharset;
    }
}
