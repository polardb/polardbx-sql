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

import java.util.Calendar;

import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.rule.model.DateEnumerationParameter;
import com.alibaba.polardbx.rule.Rule;
import com.alibaba.polardbx.rule.model.AdvancedParameter;
import com.alibaba.polardbx.rule.model.AdvancedParameter.AtomIncreaseType;
import com.alibaba.polardbx.rule.model.AdvancedParameter.Range;

/**
 * {@linkplain AdvancedParameter}解析器
 *
 * @author jianghang 2013-10-29 下午5:18:27
 * @since 5.0.0
 */
public class AdvancedParameterParser {

    public static final String PARAM_SEGMENT_SPLITOR           = ",";
    public static final char   NEED_APPEAR_SYMBOL              = '?';
    public static final String INCREASE_TYPE_SPLITOR           = "_";
    public static final String RANGE_SEGMENT_SPLITOR           = "|";
    public static final String RANGE_SEGMENT_START_END_SPLITOR = "_";

    /**
     * <pre>
     * 
     *   paramToken 
     *      定义变量的分表片段，形式类似 
     *      
     *          (col?表示为col可选列，若col是可先，则选择rule时，sql可以不包含该列。到时对该列值域做遍历)
     *          
     *          以下的规则枚举都是合法表达式：
     *          
     *          #id,1024#
     *          #id,1_number,1024#
     *          #id,1,1024# 
     *          #id,4,1024# 
     *          #id,1_number,1024|1025_2048#  
     *          #id,1_number,1_1024# 
     *          
     *          #id,100_number,1_1024# 
     *          
     *          #id,1_number?,1024# 
     *          #id,1_number,0_1024|1m_1g#
     *          #gmt_create?,1_month,-12_12#
     *          #name,1_string,a_z# 
     *          
     *   completeConfig 
     *      如果为true,那么paramToken必须满足逗号分隔的3段形式
     *      如果为false,那么paramToken可以只配置分表或者分表键 
     *          2.3.x－2.4.3的老规则配置该参数为false
     *          2.4.4后支持的新规则配置该参数为true;
     * </pre>
     * 
     * @param paramToken 定义变量的分表片段，形式类似 #gmt_create?,1_month,-12_12#
     * #id,1_number,1024# #name,1_string,a_z# #id,1_number,0_1024|1m_1g#
     * @param completeConfig 如果为true,那么paramToken必须满足逗号分隔的3段形式
     * 如果为false,那么paramToken可以只配置分表或者分表键 2.3.x－2.4.3的老规则配置该参数为false
     * 2.4.4后支持的新规则配置该参数为true;
     */
    public static AdvancedParameter getAdvancedParamByParamTokenNew(String paramToken, boolean completeConfig) {
            return getAdvancedParamByParamTokenNew(paramToken, completeConfig, null);
    }
    public static AdvancedParameter getAdvancedParamByParamTokenNew(String paramToken, boolean completeConfig, Rule exprRule) {
        String key;
        boolean[] needAppear = new boolean[1];

        AtomIncreaseType atomicIncreateType = null;
        Comparable<?> atomicIncreateValue = null;

        Range[] rangeObjectArray = null;
        Integer cumulativeTimes = null;

        String[] paramTokens = TStringUtil.split(paramToken, PARAM_SEGMENT_SPLITOR);
        switch (paramTokens.length) {
            case 1:
                if (completeConfig) {
                    throw new IllegalArgumentException(
                        "The following rules must be matched completely, like:#id,1_number,1024#");
                }
                key = parseKeyPart(paramTokens[0], needAppear);
                break;
            case 2:
                // 若只有两个，自增类型默认为number，自增值默认为1； 其他同case 3
                key = parseKeyPart(paramTokens[0], needAppear);

                atomicIncreateType = AtomIncreaseType.NUMBER;
                atomicIncreateValue = 1;

                try {
                    rangeObjectArray = parseRangeArray(paramTokens[1]);
                    cumulativeTimes = getCumulativeTimes(rangeObjectArray[0]);
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException(
                        "The input parameters are not of type Integer, the parameters are:" + paramToken, e);
                } catch (Exception e) {
                    throw new IllegalArgumentException(e);
                }

                break;
            case 3:
                key = parseKeyPart(paramTokens[0], needAppear);
                try {
                    atomicIncreateType = getIncreaseType(paramTokens[1]);
                    atomicIncreateValue = getAtomicIncreaseValue(paramTokens[1], atomicIncreateType);
                    rangeObjectArray = parseRangeArray(paramTokens[2]);
                    // 长度为三必定有范围定义，否则直接抛错
                    // 如果范围有多段("|"分割)，那么以第一段的跨度为标准
                    cumulativeTimes = getCumulativeTimes(rangeObjectArray[0]);
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException(
                        "The input parameters are not of type Integer, the parameters are:" + paramToken, e);
                } catch (Exception e) {
                    throw new IllegalArgumentException(e);
                }
                break;
            default:
                throw new IllegalArgumentException(
                    "The wrong number of parameters, must be 1 or 3. When using 3 parameters, it is allowed to use enumerated value.");
        }


        return new AdvancedParameter(key,
            atomicIncreateValue,
            cumulativeTimes,
            needAppear[0],
            atomicIncreateType,
            rangeObjectArray, exprRule);
    }

    /**
     * ColumnName?表示可选
     *
     * @param keyPart 不可能传入null
     */
    private static String parseKeyPart(String keyPart, boolean[] needAppear) {
        String key;
        keyPart = keyPart.trim();
        int endIndex = keyPart.length() - 1;
        if (keyPart.charAt(endIndex) == NEED_APPEAR_SYMBOL) {
            needAppear[0] = true;
            key = keyPart.substring(0, endIndex);
        } else {
            needAppear[0] = false;
            key = keyPart;
        }
        return key;
    }

    private static AtomIncreaseType getIncreaseType(String paramTokenStr) {
        String[] increase = TStringUtil.split(paramTokenStr.trim(), INCREASE_TYPE_SPLITOR);
        if (increase.length == 1) {
            return AtomIncreaseType.NUMBER;
        } else if (increase.length == 2) {
            return AtomIncreaseType.valueOf(increase[1].toUpperCase());
        } else if (increase.length == 3) {
            return AtomIncreaseType
                .valueOf(increase[1].toUpperCase() + INCREASE_TYPE_SPLITOR + increase[2].toUpperCase());
        } else {
            throw new IllegalArgumentException("Incremental configuration error:" + paramTokenStr);
        }
    }

    private static Comparable<?> getAtomicIncreaseValue(String paramTokenStr, AtomIncreaseType type) {
        String[] increase = TStringUtil.split(paramTokenStr.trim(), INCREASE_TYPE_SPLITOR);
        // 如果长度为1,那么默认为数字/string类型
        if (increase.length == 1) {
            return Integer.valueOf(increase[0]);
        } else if (increase.length == 2) {
            switch (type) {
                case NUMBER:
                case STRING:
                    return Integer.valueOf(increase[0]);
                case STRNUM:
                    return Integer.valueOf(increase[0]);
                case DATE:
                    return new DateEnumerationParameter(Integer.valueOf(increase[0]), Calendar.DATE);
                case WEEK:
                    return new DateEnumerationParameter(Integer.valueOf(increase[0]), Calendar.WEEK_OF_YEAR);
                case MONTH:
                    return new DateEnumerationParameter(Integer.valueOf(increase[0]), Calendar.MONTH);
                case YEAR:
                    return new DateEnumerationParameter(Integer.valueOf(increase[0]), Calendar.YEAR);
                case HOUR:
                    return new DateEnumerationParameter(Integer.valueOf(increase[0]), Calendar.HOUR_OF_DAY);
                default:
                    throw new IllegalArgumentException("Unsupported auto-increment type:" + type);
            }
        } else if (increase.length == 3) {
            switch (type) {
                case DATE_ABS:
                    return new DateEnumerationParameter(Integer.valueOf(increase[0]), Calendar.DATE);
                case MONTH_ABS:
                    return new DateEnumerationParameter(Integer.valueOf(increase[0]), Calendar.MONTH);
                case WEEK_ABS:
                    return new DateEnumerationParameter(Integer.valueOf(increase[0]), Calendar.WEEK_OF_YEAR);
                default:
                    throw new IllegalArgumentException("Unsupported auto-increment type:" + type);
            }
        } else {
            throw new IllegalArgumentException("Incremental configuration error:" + paramTokenStr);
        }
    }

    private static Range[] parseRangeArray(String paramTokenStr) {
        String[] ranges = TStringUtil.split(paramTokenStr, RANGE_SEGMENT_SPLITOR);
        Range[] rangeObjArray = new Range[ranges.length];

        for (int i = 0; i < ranges.length; i++) {
            String range = ranges[i].trim();
            String[] startEnd = TStringUtil.split(range, RANGE_SEGMENT_START_END_SPLITOR);
            if (startEnd.length == 1) {
                if (i == 0) {
                    rangeObjArray[i] = new Range(Integer.valueOf(0), Integer.valueOf(startEnd[0]));
                } else {
                    rangeObjArray[i] = new Range(fromReadableInt(startEnd[0]), fromReadableInt(startEnd[0]));
                }
            } else if (startEnd.length == 2) {
                rangeObjArray[i] = new Range(fromReadableInt(startEnd[0]), fromReadableInt(startEnd[1]));
            } else {
                throw new IllegalArgumentException("Range definition error," + paramTokenStr);
            }
        }
        return rangeObjArray;
    }

    /**
     * 1m = 1,000,000; 2M = 2,000,000 1g = 1,000,000,000 3G = 3,000,000,000
     */
    private static int fromReadableInt(String readableInt) {
        char c = readableInt.charAt(readableInt.length() - 1);
        if (c == 'm' || c == 'M') {
            return Integer.valueOf(readableInt.substring(0, readableInt.length() - 1)) * 1000000;
        } else if (c == 'g' || c == 'G') {
            return Integer.valueOf(readableInt.substring(0, readableInt.length() - 1)) * 1000000000;
        } else {
            return Integer.valueOf(readableInt);
        }
    }

    private static Integer getCumulativeTimes(Range ro) {
        return ro.end - ro.start;
    }
}
