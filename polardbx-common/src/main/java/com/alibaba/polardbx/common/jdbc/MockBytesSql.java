package com.alibaba.polardbx.common.jdbc;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
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
public class MockBytesSql extends BytesSql {

    /**
     * built by bytes array, normal way
     */
    public MockBytesSql(byte[][] bytesArray, boolean parameterLast) {
        super(bytesArray, parameterLast);
    }

    /**
     * bytes to String.
     */
    public String toString(List<ParameterContext> parameterContexts) {
        StringBuilder sb = new StringBuilder();
        int paramIndex = 0;
        byte[][] bytes = getBytesArray();
        for (int i = 0; i < bytes.length; i++) {
            sb.append(new String(bytes[i], Charset.defaultCharset()));
            if ((i == bytes.length - 1) && !isParameterLast()) {
                break;
            }
            Object o = parameterContexts.get(paramIndex).getValue();
            if (o instanceof Number) {
                sb.append(o);
            } else if (o instanceof RawString) {
                sb.append(((RawString) o).buildRawString());
            } else if (o instanceof byte[]) {
                sb.append('x').append(TStringUtil.quoteString(TStringUtil.bytesToHexString((byte[]) o)));
            } else if (o == null) {
                sb.append("null");
            } else {
                sb.append(TStringUtil.quoteString(o.toString()));
            }
            paramIndex++;
        }
        return sb.toString();
    }

}
