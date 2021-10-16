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

package com.alibaba.polardbx.rule.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 一个简单的有名字的占位符替换器 类似java.text.MessageFormat, 只是占位符是{name1} {name2} 而不是{0} {2}
 * 
 * @author linxuan
 */
public class SimpleNamedMessageFormat {

    private static final String DEFAULT_PLACEHOLDER_PREFIX = "{";
    private static final String DEFAULT_PLACEHOLDER_SUFFIX = "}";

    private final String        pattern;
    private final String        placeholderPrefix;
    private final String        placeholderSuffix;

    private volatile boolean    parsed;
    private volatile List<Frag> frags;

    private static class Frag {

        public final String  value;
        public final boolean isPlaceHolderName;

        public Frag(String piece, boolean isPlaceHolderName){
            this.value = piece;
            this.isPlaceHolderName = isPlaceHolderName;
        }
    }

    public SimpleNamedMessageFormat(String pattern){
        this.pattern = pattern;
        this.placeholderPrefix = DEFAULT_PLACEHOLDER_PREFIX;
        this.placeholderSuffix = DEFAULT_PLACEHOLDER_SUFFIX;
    }

    public SimpleNamedMessageFormat(String pattern, String placeholderPrefix, String placeholderSuffix){
        this.pattern = pattern;
        this.placeholderPrefix = placeholderPrefix;
        this.placeholderSuffix = placeholderSuffix;
    }

    public String format(Map<String, ? extends Object> args) {
        if (parsed && frags != null) {
            return buildByParsedFrags(args);
        } else {
            return format0(args);
        }
    }

    private String buildByParsedFrags(Map<String, ? extends Object> args) {
        // 实际上这个sb的长度可以提前算出来(guangxia)
        StringBuilder sb = new StringBuilder();
        for (Frag frag : frags) {
            if (!frag.isPlaceHolderName) {
                sb.append(frag.value);
                continue;
            }
            Object arg = args.get(frag.value);
            if (arg != null) sb.append(arg);
            else {
                sb.append(this.placeholderPrefix).append(frag.value).append(this.placeholderSuffix);
            }
        }
        return sb.toString();
    }

    /**
     * 不支持嵌套
     */
    private String format0(Map<String, ? extends Object> args) {
        List<Frag> initfrags = new ArrayList<Frag>();
        int cursor = 0;
        int index0 = this.pattern.indexOf(placeholderPrefix);
        int index1 = this.pattern.indexOf(placeholderSuffix);
        while (index0 != -1 && index1 != -1) {
            initfrags.add(new Frag(this.pattern.substring(cursor, index0), false));
            initfrags.add(new Frag(this.pattern.substring(index0 + placeholderPrefix.length(), index1), true));

            cursor = index1 + placeholderSuffix.length();
            index0 = this.pattern.indexOf(placeholderPrefix, cursor);
            index1 = this.pattern.indexOf(placeholderSuffix, index0 + placeholderPrefix.length());
        }
        initfrags.add(new Frag(this.pattern.substring(cursor), false));
        this.frags = initfrags;
        this.parsed = true;
        return buildByParsedFrags(args);
    }
}
