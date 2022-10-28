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

package com.alibaba.polardbx.common.jdbc;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import com.google.protobuf.ByteString;
import org.apache.commons.lang.StringUtils;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.alibaba.polardbx.common.utils.GeneralUtil.findStartOfStatement;

/**
 * Store sql using byte[][], byte array split by ?(parameter)
 * encoding by UTF8
 *
 * @author jilong.ljl
 */
public class BytesSql {

    private static final int MAX_CACHE_SQL_SIZE = 3000;
    private static final int MAX_CACHE_SIZE = 500;
    private static final int CACHE_EXPIRE_TIME = 12 * 3600 * 1000; // 12h

    /**
     * `sql` cache
     */
    private static LoadingCache<String, BytesSql> cacheForInternalSql = buildCache(MAX_CACHE_SIZE);

    /**
     * byte[] array split by ?(parameter), if `sql` endwith ?(parameter), then set parameterLast with true
     */
    @JsonProperty
    private byte[][] bytesArray;

    /**
     * if `sql` endwith ?(parameter)
     */
    @JsonProperty
    private boolean parameterLast;

    /**
     * digest cache
     */
    private transient ByteString digest;

    private transient byte[] originCache;

    /**
     * built by bytes array, normal way
     */
    @JsonCreator
    public BytesSql(@JsonProperty("bytesArray") byte[][] bytesArray,
                    @JsonProperty("parameterLast") boolean parameterLast) {
        this.bytesArray = bytesArray;
        this.parameterLast = parameterLast;
    }

    private static LoadingCache<String, BytesSql> buildCache(long maxSize) {
        return CacheBuilder.newBuilder()
            .maximumSize(maxSize)
            .expireAfterWrite(CACHE_EXPIRE_TIME, TimeUnit.MILLISECONDS)
            .softValues()
            .build(new CacheLoader<String, BytesSql>() {
                @Override
                public BytesSql load(String key) throws Exception {
                    return buildBytesSql(key);
                }
            });
    }

    /**
     * Transform one sql(string) to BytesSql
     */
    public static BytesSql buildBytesSql(String sql) {
        // Dealing sql.
        final int startPos = findStartOfStatement(sql);
        final int statementLength = sql.length();
        final char quotedIdentifierChar = '`';

        boolean inQuotes = false;
        char quoteChar = 0;
        boolean inQuotedId = false;

        int lastSplit = startPos;
        List<byte[]> bytesList = Lists.newLinkedList();
        boolean parameterLast = false;
        for (int i = startPos; i < statementLength; ++i) {
            char c = sql.charAt(i);
            parameterLast = false;
            if (c == '\\' && i < (statementLength - 1)) {
                i++;
                continue; // next character is escaped
            }

            // are we in a quoted identifier? (only valid when the id is not inside a 'string')
            if (!inQuotes && (c == quotedIdentifierChar)) {
                inQuotedId = !inQuotedId;
            } else if (!inQuotedId) {
                //	only respect quotes when not in a quoted identifier

                if (inQuotes) {
                    if (((c == '\'') || (c == '"')) && c == quoteChar) {
                        if (i < (statementLength - 1) && sql.charAt(i + 1) == quoteChar) {
                            i++;
                            continue; // inline quote escape
                        }

                        inQuotes = false;
                        quoteChar = 0;
                    }
                } else {
                    if (c == '#' || (c == '-' && (i + 1) < statementLength && sql.charAt(i + 1) == '-')) {
                        // run out to end of statement, or newline, whichever comes first
                        int endOfStmt = statementLength - 1;

                        for (; i < endOfStmt; i++) {
                            c = sql.charAt(i);

                            if (c == '\r' || c == '\n') {
                                break;
                            }
                        }

                        continue;
                    } else if (c == '/' && (i + 1) < statementLength) {
                        // Comment?
                        char cNext = sql.charAt(i + 1);

                        if (cNext == '*') {
                            i += 2;

                            for (int j = i; j < statementLength; j++) {
                                i++;
                                cNext = sql.charAt(j);

                                if (cNext == '*' && (j + 1) < statementLength) {
                                    if (sql.charAt(j + 1) == '/') {
                                        i++;

                                        if (i < statementLength) {
                                            c = sql.charAt(i);
                                        }

                                        break; // comment done
                                    }
                                }
                            }
                        }
                    } else if ((c == '\'') || (c == '"')) {
                        inQuotes = true;
                        quoteChar = c;
                    }
                }
            }

            if ((c == '?') && !inQuotes && !inQuotedId) {
                // split
                bytesList.add(sql.substring(lastSplit, i).getBytes(StandardCharsets.UTF_8));
                // +1 meaning we dont need ?
                lastSplit = i + 1;
                parameterLast = true;
            }
        }
        if (lastSplit != sql.length()) {
            bytesList.add(sql.substring(lastSplit).getBytes(StandardCharsets.UTF_8));
        }
        byte[][] bytesArray = bytesList.toArray(new byte[0][]);
        BytesSql b = new BytesSql(bytesArray, parameterLast);
        return b;
    }

    /**
     * Try get BytesSql by sql(string) from cache
     */
    public static BytesSql getBytesSql(String sql) {
        // avoid cache sql with too much length
        if (sql.length() > MAX_CACHE_SQL_SIZE) {
            return buildBytesSql(sql);
        }
        try {
            return cacheForInternalSql.get(sql);
        } catch (ExecutionException e) {
            e.printStackTrace();
            throw new RuntimeException("bytes sql build failed");
        }
    }

    /**
     * return the size of parameters
     */
    public int dynamicSize() {
        if (parameterLast) {
            return getBytesArray().length;
        }
        return getBytesArray().length - 1;
    }

    /**
     * Join byte[][] to one byte array, decode it if charset is different.
     */
    public byte[] getBytes(Charset charset) {
        byte[] origin = getBytes();
        if (charset == StandardCharsets.UTF_8) {
            return origin;
        }

        return StandardCharsets.UTF_8.decode(ByteBuffer.wrap(origin)).toString().getBytes(charset);
    }

    // now we don't support compact n-D array param
    public boolean containRawString(List<ParameterContext> parameterContexts) {
        if (null == parameterContexts) {
            return false;
        }
        final int len = Math.min(bytesArray.length, parameterContexts.size());
        for (int i = 0; i < len; ++i) {
            if (parameterContexts.get(i).getValue() instanceof RawString) {
                return true;
            }
        }
        return false;
    }

    public boolean containRawString(Map<Integer, ParameterContext> param) {
        if (null == param) {
            return false;
        }
        for (Map.Entry<Integer, ParameterContext> entry : param.entrySet()) {
            if (entry.getValue().getValue() instanceof RawString) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns a new byte[] composed of copies of the bytesArray elements joined together with a copy of the specified delimiter '?'.
     */
    public byte[] getBytes(List<ParameterContext> parameterContexts) {
        int var1 = 0;

        int rawSize = 1;
        for (int var2 = 0; var2 != getBytesArray().length; ++var2) {

            if (parameterContexts != null
                && parameterContexts.size() > var2
                && parameterContexts.get(var2).getValue() != null
                && parameterContexts.get(var2)
                .getValue() instanceof RawString) {
                int step = 2;
                Object args0 = ((RawString) parameterContexts.get(var2).getValue()).getObjList().get(0);
                if (args0 instanceof List) {
                    step = 3 + ((List<?>) args0).size() * 2 - 1;
                }
                rawSize = ((RawString) parameterContexts.get(var2).getValue()).size() * step - 1;
            } else {
                rawSize = 1;
            }
            var1 += getBytesArray()[var2].length + rawSize;
        }
        if (!parameterLast) {
            var1--;
        }

        byte[] var5 = new byte[var1];
        int var3 = 0;

        for (int var4 = 0; var4 != getBytesArray().length; ++var4) {
            System.arraycopy(getBytesArray()[var4], 0, var5, var3, getBytesArray()[var4].length);
            var3 += getBytesArray()[var4].length;
            int questSize = 1;

            if (parameterContexts != null
                && parameterContexts.size() > var4
                && parameterContexts.get(var4).getValue() != null
                && parameterContexts.get(var4).getValue() instanceof RawString) {
                rawSize = ((RawString) parameterContexts.get(var4).getValue()).size();
                Object args0 = ((RawString) parameterContexts.get(var4).getValue()).getObjList().get(0);
                if (args0 instanceof List) {
                    questSize = ((List<?>) args0).size();
                }
            } else {
                rawSize = 1;
            }

            if (var4 != getBytesArray().length - 1) {
                writeQuest(var5, rawSize, questSize, var3);
                if (questSize == 1) {
                    var3 += rawSize * 2 - 1;
                } else {
                    var3 += rawSize * (3 + 2 * questSize - 1) - 1;
                }
            } else if (parameterLast) {
                writeQuest(var5, rawSize, questSize, var3);
                break;
            }
        }

        return var5;
    }

    private void writeQuest(byte[] var5, int rawSize, int questSize, int var3) {
        if (questSize == 1) {
            for (int i = 0; i < rawSize; i++) {
                var5[var3] = '?';
                if (var5.length > var3 + 1) {
                    var5[var3 + 1] = ',';
                }
                var3 += 2;
            }
        } else {
            for (int i = 0; i < rawSize; i++) {
                var5[var3++] = '(';
                for (int j = 0; j < questSize; j++) {
                    var5[var3] = '?';
                    var5[var3 + 1] = ',';
                    var3 += 2;
                }
                var5[var3 - 1] = ')';
                var5[var3] = ',';
                var3 += 1;
            }
        }
    }

    /**
     * Returns a new byte[] composed of copies of the bytesArray elements joined together with a copy of the specified delimiter '?'.
     */
    @JsonIgnore
    public byte[] getBytes() {
        if (originCache != null) {
            return originCache;
        }
        int var1 = 0;

        for (int var2 = 0; var2 != getBytesArray().length; ++var2) {
            var1 += getBytesArray()[var2].length + 1;
        }
        if (!parameterLast) {
            var1--;
        }

        byte[] var5 = new byte[var1];
        int var3 = 0;

        for (int var4 = 0; var4 != getBytesArray().length; ++var4) {
            System.arraycopy(getBytesArray()[var4], 0, var5, var3, getBytesArray()[var4].length);
            var3 += getBytesArray()[var4].length;

            if (var4 != getBytesArray().length - 1) {
                var5[var3] = '?';
                var3++;
            } else if (parameterLast) {
                var5[var3] = '?';
                break;
            }
        }
        originCache = var5;
        return var5;
    }

    /**
     * bytes to String.
     */
    public String toString(List<ParameterContext> parameterContexts) {
        return new String(getBytes(parameterContexts), StandardCharsets.UTF_8);
    }

    @Override
    public String toString() {
        // should not use directly
        byte[] bytes = getBytes();
        return new String(bytes, StandardCharsets.UTF_8);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (obj instanceof BytesSql) {
            final BytesSql sql = (BytesSql) obj;
            if (this.bytesArray.length != sql.bytesArray.length || this.parameterLast != sql.parameterLast) {
                return false;
            }
            // compare bytes internal
            for (int i = 0; i < this.bytesArray.length; ++i) {
                final byte[] left = this.bytesArray[i];
                final byte[] right = sql.bytesArray[i];
                if (!Arrays.equals(left, right)) {
                    return false;
                }
            }
        }
        return false;
    }

    /**
     * Return a String for log or display
     */
    public String display() {
        byte[] bytes = getBytes();
        String s = new String(bytes, StandardCharsets.UTF_8);
        if (s.length() > 4096) {
            s = s.substring(0, 4096);
        }
        return s.replaceAll("\n", " ");
    }

    /**
     * return the mem cost of BytesSql obj
     */
    public int size() {
        int var1 = 0;

        for (int var2 = 0; var2 != getBytesArray().length; ++var2) {
            var1 += getBytesArray()[var2].length + 1;
        }
        return var1;
    }

    /**
     * if BytesSql end with Parameter
     */
    @JsonProperty
    public boolean isParameterLast() {
        return parameterLast;
    }

    /**
     * byte[] array split by ?(parameter), if `sql` endwith ?(parameter), then set parameterLast with true
     */
    @JsonProperty
    public byte[][] getBytesArray() {
        return bytesArray;
    }

    /**
     * BytesSql builder, used to build one BytesSql in a sql
     * visitor.
     */
    public static class BytesSqlBuilder {
        private List<byte[]> bytesList = Lists.newLinkedList();
        private boolean parameterLast = false;

        private BytesSqlBuilder() {
        }

        public static BytesSqlBuilder getInstance() {
            return new BytesSqlBuilder();
        }

        public BytesSql build() {
            return new BytesSql(bytesList.toArray(new byte[0][]), parameterLast);
        }

        public void setParameterLast() {
            parameterLast = true;
        }

        public void write(byte[] bytes, Charset charset) {
            if (charset != StandardCharsets.UTF_8) {
                bytesList.add(new String(bytes, charset).getBytes(StandardCharsets.UTF_8));
            } else {
                bytesList.add(bytes);
            }
        }

    }

    /**
     * bytestring for x-protocol
     */
    public ByteString byteString(String encoding) {
        if (encoding == null || StringUtils.isEmpty(encoding)) {
            return byteString("utf8");
        }

        return ByteString.copyFrom(getBytes(Charset.forName(encoding)));
    }

    /**
     * degest for x-protocol
     */
    public ByteString digest() throws NoSuchAlgorithmException {
        // cache for reusing
        if (digest != null) {
            return digest;
        }
        digest = com.google.protobuf.ByteString
            .copyFrom(MessageDigest.getInstance("md5").digest(getBytes(StandardCharsets.UTF_8)));
        return digest;
    }
}
