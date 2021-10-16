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
import java.nio.charset.spi.CharsetProvider;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

public class TCharsetProvider extends CharsetProvider {

    private Map<String, Charset> charsetMap;
    private Collection<Charset> charsets;

    public TCharsetProvider() {
        init();
    }

    @Override
    public Charset charsetForName(String charsetName) {
        return charsetMap.get(charsetName.toLowerCase());
    }

    @Override
    public Iterator<Charset> charsets() {
        return charsets.iterator();
    }

    private void init() {
        charsetMap = new HashMap<>();
        charsets = new HashSet<>();

        charsets.add(WrappedCharset.of(CharsetName.BINARY));
        charsets.add(WrappedCharset.of(CharsetName.UTF8MB4));
        for (Charset charset : charsets) {
            charsetMap.put(charset.name().toLowerCase(), charset);
            for (String aliase : charset.aliases()) {
                charsetMap.put(aliase.toLowerCase(), charset);
            }
        }
    }
}
