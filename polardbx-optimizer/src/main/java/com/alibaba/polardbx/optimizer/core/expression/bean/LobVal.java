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

package com.alibaba.polardbx.optimizer.core.expression.bean;

import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.optimizer.parse.util.ParseString;

import java.io.UnsupportedEncodingException;
import java.util.Arrays;

/**
 * 带前缀的字符串, 例如：x'20', b'1100', _utf8'x', _latin1'x', _binary'u'.
 *
 * @since 5.0.0
 */
public class LobVal implements Comparable<String> {

    private Object value;
    private String string;
    private String introducer;
    private String charset;

    public LobVal(String string, String introducer, String charset) {
        this.string = string;
        this.introducer = introducer;
        this.charset = charset;
    }

    @Override
    public int compareTo(String o) {
        return this.string.compareTo(o);
    }

    public String getString() {
        return string;
    }

    public String getIntroducer() {
        return introducer;
    }

    @Override
    public String toString() {
        return introducer + '\'' + string + '\'';
    }

    public Object evaluation() {
        if (value != null) {
            return value;
        }
        if ("x".equals(introducer)) {
            // 解码 16 进制
            value = ParseString.hexString2Bytes(string);
        } else if (introducer.charAt(0) == '_') {
            value = charsetEncode(TStringUtil.getUnescapedString(string), charset, introducer.substring(1));
        } else {
            throw new NotSupportException(introducer + "'" + string + "'");
        }
        return value;
    }

    public static String charsetEncode(String string, String charset, String encoding) {
        if ("binary".equals(encoding)) {
            encoding = "latin1";
        }
        if (charset != null && !charset.equals(encoding)) {
            try {
                return new String(string.getBytes(charset), encoding);
            } catch (UnsupportedEncodingException e) {
                throw new NotSupportException("Unsupported encoding: _" + encoding);
            }
        }
        return string;
    }

    @Override
    public int hashCode() {
        Object value = evaluation();
        if (value instanceof byte[]) {
            return Arrays.hashCode((byte[]) value);
        }
        return value.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        LobVal other = (LobVal) obj;
        if (introducer == null) {
            if (other.introducer != null) {
                return false;
            }
        } else if (!introducer.equals(other.introducer)) {
            return false;
        }
        if (string == null) {
            if (other.string != null) {
                return false;
            }
        } else if (!string.equals(other.string)) {
            return false;
        }
        return true;
    }
}
