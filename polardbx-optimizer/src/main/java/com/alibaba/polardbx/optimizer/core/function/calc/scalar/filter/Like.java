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

package com.alibaba.polardbx.optimizer.core.function.calc.scalar.filter;

import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractCollationScalarFunction;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.exception.FunctionException;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @since 5.0.0
 */
public class Like extends AbstractCollationScalarFunction {

    public Like() {
        super(null, null);
    }

    public Like(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    public static char[] regexSpecialChars = {
        '<', '>', '^', '$', '/', ';', '(', ')', '?', '.',
        '*', '[', ']', '+', '|'};

    private static Cache<String, Pattern> patterns = CacheBuilder.newBuilder()
        .maximumSize(1000)
        .expireAfterWrite(5 * 60 * 1000, TimeUnit.MILLISECONDS)
        .softValues()
        .build();

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object arg : args) {
            if (FunctionUtils.isNull(arg)) {
                return 0l;
            }
        }
        String escape = "\\";
        if (args.length > 2 && args[2] != null) {
            escape = DataTypeUtil.convert(operandTypes.get(2), DataTypes.StringType, args[2]);
        }

        String left = DataTypeUtil.convert(operandTypes.get(0), DataTypes.StringType, args[0]);
        final String right = DataTypeUtil.convert(operandTypes.get(1), DataTypes.StringType, args[1]);
        final String escTmp = escape;
        try {
            Pattern pattern = patterns.get(buildKey(right, escTmp), new Callable<Pattern>() {

                @Override
                public Pattern call() throws Exception {
                    return Pattern.compile(buildPattern(right, escTmp), Pattern.CASE_INSENSITIVE);
                }
            });
            Matcher m = pattern.matcher(left);
            return m.matches() ? 1L : 0L;
        } catch (ExecutionException e) {
            throw new FunctionException(e.getCause());
        }
    }

    public boolean like(String str, String like) {
        final String escTmp = "\\";
        try {
            Pattern pattern = patterns.get(buildKey(like, escTmp), new Callable<Pattern>() {

                @Override
                public Pattern call() throws Exception {
                    return Pattern.compile(buildPattern(like, escTmp), Pattern.CASE_INSENSITIVE);
                }
            });
            Matcher m = pattern.matcher(str);
            return m.matches();
        } catch (ExecutionException e) {
            throw new FunctionException(e.getCause());
        }
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"LIKE"};
    }

    private String buildKey(String pattern, String escape) {
        StringBuilder builder = new StringBuilder();
        builder.append(pattern).append((char) 1).append(escape);
        return builder.toString();
    }

    private String buildPattern(String pattern, String escape) {
        char esc = escape.charAt(0);
        pattern = TStringUtil.trim(pattern);
        StringBuilder builder = new StringBuilder("^");
        int index = 0, last = 0;
        while (true) {
            // 查找esc
            index = pattern.indexOf(esc, last);
            if (index == -1 || index >= pattern.length()) {
                if (last < pattern.length()) {
                    builder.append(convertWildcard(ripRegex(pattern.substring(last))));
                }
                break;
            }
            if (index > 0) {
                String toRipRegex = TStringUtil.substring(pattern, last, index);
                builder.append(convertWildcard(ripRegex(toRipRegex)));
                last = index;
            }
            if (index + 1 < pattern.length()) {
                builder.append(ripRegex(pattern.charAt(index + 1)));
                last = index + 2;
            } else {
                builder.append(pattern.charAt(index));
                last = index + 1;
            }
            if (last >= pattern.length()) {
                break;
            }
        }
        builder.append('$');
        return builder.toString();
    }

    private String ripRegex(char toRip) {
        char[] chars = new char[1];
        chars[0] = toRip;
        return ripRegex(new String(chars));
    }

    private String ripRegex(String toRip) {
        // 反斜杠必须首先转义, 否则后面转义其他字符会插入反斜杠
        toRip = TStringUtil.escape(toRip, '\\', '\\');

        for (char ch : regexSpecialChars) {
            toRip = TStringUtil.escape(toRip, ch, '\\');
        }

        return toRip;
    }

    private String convertWildcard(String toConvert) {
        toConvert = TStringUtil.replace(toConvert, "_", "([\\s\\S])");
        toConvert = TStringUtil.replace(toConvert, "%", "([\\s\\S]*)");
        return toConvert;
    }

}
