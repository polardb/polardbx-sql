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

import java.text.ParseException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Map;

import com.alibaba.polardbx.common.exception.NotSupportException;
import org.apache.commons.lang.time.DateFormatUtils;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.model.sqljep.Comparative;
import com.alibaba.polardbx.common.model.sqljep.ComparativeAND;
import com.alibaba.polardbx.common.model.sqljep.ComparativeBaseList;
import com.alibaba.polardbx.common.model.sqljep.ComparativeOR;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.TreeMaps;

/**
 * 提供一种机制，允许业务自定义condition，通过该解析类转化为Rule锁需要的{@linkplain Comparative}对象
 * 
 * <pre>
 * 简单语法： KEY CMP VALUE [:TYPE]
 * 1. KEY： 类似字段名字，用户随意定义
 * 2. CMP： 链接符，比如< = > 等，具体可查看{@linkplain Comparative}
 * 3. VALUE: 对应的值，比如1
 * 4. TYPE: 描述VALUE的类型，可选型，如果不填默认为Long类型。支持: int/long/string/date，可以使用首字母做为缩写，比如i/l/s/d。
 * 
 * 几个例子：
 * 1. id = 1
 * 2. id = 1 : long
 * 3. id > 1 and id < 1 : long
 * 4. gmt_create = 2011-11-11 : date
 * 5. id in (1,2,3,4) : long
 * </pre>
 * 
 * @author jianghang 2013-11-6 下午5:23:02
 * @since 5.0.0
 */
public class ComparativeStringAnalyser {

    private static final String   TYPE_SPLIT   = ":";
    private static final String[] DATE_FORMATS = new String[] { "yyyy-MM-dd", "HH:mm:ss", "yyyy-MM-dd HH:mm:ss",
            "yyyy-MM-dd hh:mm:ss.S", "EEE MMM dd HH:mm:ss zzz yyyy", DateFormatUtils.ISO_DATETIME_FORMAT.getPattern(),
            DateFormatUtils.ISO_DATETIME_TIME_ZONE_FORMAT.getPattern(),
            DateFormatUtils.SMTP_DATETIME_FORMAT.getPattern(), };

    public static Map<String, Comparative> decodeComparativeString2Map(String conditionStr) {
        Map<String, Comparative> comparativeMap = TreeMaps.caseInsensitiveMap();
        // 处理 ComparativeMap
        String[] comStrs = conditionStr.split(";");
        for (int i = 0; i < comStrs.length; i++) {
            String value = comStrs[i];
            if (TStringUtil.isNotBlank(value)) {
                boolean containsAnd = TStringUtil.contains(value, " and ");
                boolean containsOr = TStringUtil.contains(value, " or ");
                if (containsAnd || containsOr) {
                    ComparativeBaseList comparativeBaseList = null;
                    String op;
                    if (containsOr) {
                        comparativeBaseList = new ComparativeOR();
                        op = "or";
                    } else if (containsAnd) {
                        comparativeBaseList = new ComparativeAND();
                        op = "and";
                    } else {
                        throw new TddlRuntimeException(ErrorCode.ERR_ROUTE,
                            "decodeComparative not support ComparativeBaseList value:" + value);
                    }
                    String[] compValues = TStringUtil.twoPartSplit(value, op);
                    String key = null;
                    for (String compValue : compValues) {
                        Comparative comparative = decodeComparative(compValue, null);
                        if (null != comparative) {
                            comparativeBaseList.addComparative(comparative);
                        }
                        String temp = decodeComparativeKey(compValue).trim();
                        if (null == key) {
                            key = temp;
                        } else if (!temp.equals(key)) {
                            throw new RuntimeException("decodeComparative not support ComparativeBaseList value:"
                                                       + value);
                        }
                    }
                    comparativeMap.put(key, comparativeBaseList);
                } else {
                    // 说明只是Comparative
                    String key = decodeComparativeKey(value);
                    Comparative comparative = decodeComparative(value, null);
                    if (null != comparative) {
                        comparativeMap.put(key, comparative);
                    }
                }
            }
        }
        return comparativeMap;
    }

    /**
     * 解析类似: =1:int or in(1,2,3):long
     * 
     * @param compValue
     * @return
     */
    public static Comparative decodeComparative(String compValue, String globalType) {
        boolean containsIn = TStringUtil.contains(compValue, " in");
        Comparative comparative = null;
        if (!containsIn) {
            int compEnum = Comparative.getComparisonByCompleteString(compValue);
            String splitor = Comparative.getComparisonName(compEnum);
            int size = splitor.length();
            int index = compValue.indexOf(splitor);
            String valueTypeStr = TStringUtil.substring(compValue, index + size);
            int lastColonIndex = valueTypeStr.lastIndexOf(TYPE_SPLIT);
            if (lastColonIndex != -1) {
                String value = valueTypeStr.substring(0, lastColonIndex);
                String type = valueTypeStr.substring(lastColonIndex + 1);
                comparative = decodeComparative(type, compEnum, value);
            } else { // 如果不存在类型
                comparative = decodeComparative(globalType, compEnum, valueTypeStr);
            }
        } else {
            // 处理下in表达式
            String[] compValues = TStringUtil.twoPartSplit(compValue, " in");
            int lastColonIndex = compValues[1].lastIndexOf(TYPE_SPLIT);
            String inValues = compValues[1];
            String type = null;
            if (lastColonIndex != -1) {
                inValues = compValues[1].substring(0, lastColonIndex);
                type = compValues[1].substring(lastColonIndex + 1);
            } else {
                inValues = compValues[1];
                type = globalType;
            }

            ComparativeOR comparativeBaseList = new ComparativeOR();
            String[] values = TStringUtil.split(TStringUtil.getBetween(inValues, "(", ")"), ",");
            for (String value : values) {
                Comparative cmp = decodeComparative(type, Comparative.Equivalent, value);
                comparativeBaseList.addComparative(cmp);
            }
            comparative = comparativeBaseList;
        }

        return comparative;
    }

    /**
     * 根据类型解析下数据
     * 
     * @param type
     * @param compEnum
     * @param value
     * @return
     */
    private static Comparative decodeComparative(String type, int compEnum, String value) {
        if (value == null) {
            throw new RuntimeException("decodeComparative Error notSupport Comparative valueType value: " + value
                                       + TYPE_SPLIT + type);
        }

        if (type == null) {
            type = "l"; // 默认按照long来处理
        }

        Comparative comparative = null;
        if ("i".equalsIgnoreCase(type.trim()) || "int".equalsIgnoreCase(type.trim())) {
            comparative = new Comparative(compEnum, Integer.valueOf(value.trim()));
        } else if ("l".equalsIgnoreCase(type.trim()) || "long".equalsIgnoreCase(type.trim())) {
            comparative = new Comparative(compEnum, Long.valueOf(value.trim()));
        } else if ("s".equalsIgnoreCase(type.trim()) || "string".equalsIgnoreCase(type.trim())) {
            comparative = new Comparative(compEnum, value.trim());
        } else if ("d".equalsIgnoreCase(type.trim()) || "date".equalsIgnoreCase(type.trim())) {
            Date date = null;
            try {
                date = parseDate(value.trim(), DATE_FORMATS, Locale.ENGLISH);
            } catch (Exception err) {
                try {
                    date = parseDate(value.trim(), DATE_FORMATS, Locale.getDefault());
                } catch (Exception e) {
                    throw GeneralUtil.nestedException("unSupport date parse :" + value.trim());
                }
            }

            comparative = new Comparative(compEnum, date);
        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_ROUTE,
                "decodeComparative Error notSupport Comparative valueType value: " + value + TYPE_SPLIT + type);
        }

        return comparative;
    }

    /**
     * 获取对应的key，类似id=xx中的id信息
     */
    public static String decodeComparativeKey(String compValue) {
        boolean containsIn = TStringUtil.contains(compValue, " in");
        if (containsIn) {
            String[] compValues = TStringUtil.twoPartSplit(compValue, " in");
            return compValues[0].trim();
        } else {
            int value = Comparative.getComparisonByCompleteString(compValue);
            String splitor = Comparative.getComparisonName(value);
            int index = compValue.indexOf(splitor);
            return TStringUtil.substring(compValue, 0, index).trim();
        }
    }

    private static Date parseDate(String str, String[] parsePatterns, Locale locale) throws ParseException {
        if ((str == null) || (parsePatterns == null)) {
            throw new IllegalArgumentException("Date and Patterns must not be null");
        }

        SimpleDateFormat parser = null;
        ParsePosition pos = new ParsePosition(0);

        for (int i = 0; i < parsePatterns.length; i++) {
            if (i == 0) {
                parser = new SimpleDateFormat(parsePatterns[0], locale);
            } else {
                parser.applyPattern(parsePatterns[i]);
            }
            pos.setIndex(0);
            Date date = parser.parse(str, pos);
            if ((date != null) && (pos.getIndex() == str.length())) {
                return date;
            }
        }

        throw new NotSupportException("Unable to parse the date: " + str);
    }

}
