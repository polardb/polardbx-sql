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

package com.alibaba.polardbx.optimizer.parse.hint;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.model.Group;
import com.alibaba.polardbx.common.model.hint.DirectlyRouteCondition;
import com.alibaba.polardbx.common.model.hint.ExtraCmdRouteCondition;
import com.alibaba.polardbx.common.model.hint.FullRouteCondition;
import com.alibaba.polardbx.common.model.hint.RouteCondition;
import com.alibaba.polardbx.common.model.hint.RuleRouteCondition;
import com.alibaba.polardbx.common.model.sqljep.Comparative;
import com.alibaba.polardbx.common.model.sqljep.ComparativeAND;
import com.alibaba.polardbx.common.model.sqljep.ComparativeBaseList;
import com.alibaba.polardbx.common.model.sqljep.ComparativeOR;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.exception.SqlParserException;
import com.alibaba.polardbx.optimizer.parse.HintParser;
import com.alibaba.polardbx.rule.utils.ComparativeStringAnalyser;
import org.apache.commons.lang.StringUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 简单hint解析
 *
 * <pre>
 * 完整的例子：
 *
 *  \/*+TDDL({"type":"condition","vtab":"vtabxxx","params":[{"relation":"and","expr":["pk>4","pk:long<10"],"paramtype":"int"}],"extra":"{"ALLOW_TEMPORARY_TABLE"="TRUE"})*\/
 * 1. type取值是condition/direct/full
 * 2. vtab取值是rule规则中的逻辑表名
 * 3. params对应于rule规则中的分库字段的条件
 *    a. relation取值是and和or,可以不用，但是expr里面元素必须为一个。（即where pk>4）
 *    b. expr对应为分库条件
 *    c. paramtype对应分库子段类型
 * 4. extra取值是针对{@linkplain ConnectionProperties}中定义的扩展参数，比如ALLOW_TEMPORARY_TABLE=true代表开启临时表
 *
 * type为direct时
 * a. \/*+TDDL({"type":"direct","vtab":"real_tab","dbid":"xxx_group","realtabs":["real_tab_0","real_tab_1"]})*\/select * from real_tab;
 * 绕过解析器, 进行表名替换,然后在对应group ds上执行
 * b. \/*+TDDL({"type":"direct","dbid":"xxx_group"})*\/select * from real_table_0;
 * 直接将sql在对应group ds上执行
 * </pre>
 *
 * @author jianghang 2014-1-13 下午6:16:31
 * @since 5.0.0
 */
public class SimpleHintParser {

    private static final Logger logger = LoggerFactory.getLogger(SimpleHintParser.class);
    public static final String TDDL_HINT_PREFIX = "/*+TDDL(";
    public static final String TDDL_HINT_SEMICOLON_PREFIX = ";/*+TDDL(";
    public static final String TDDL_HINT_END = ")*/";
    public static final String TDDL_GROUP_HINT_PREFIX = "/*+TDDL_GROUP({";
    public static final String TDDL_GROUP_HINT_END = "})*/";
    public static final String TDDL_HINT_UGLY_PREFIX = "/!+TDDL";
    public static final String TDDL_HINT_UGLY_PREFIX_COMMENT = "/*+TDDL";
    public static final String TYPE = "type";
    public static final String TYPE_CONDITION = "condition";
    public static final String TYPE_FILTER = "filter";

    public static final String TYPE_DIRECT = "direct";
    public static final String TYPE_FULL = "full";
    public static final String OR = "or";
    public static final String AND = "and";
    public static final String RELATION = "relation";
    public static final String PARAMTYPE = "paramtype";
    public static final String EXPR = "expr";
    public static final String PARAMS = "params";
    public static final String VTAB = "vtab";
    public static final String DBID = "dbid";
    public static final String DBINDEX = "dbindex";
    public static final String REALTABS = "realtabs";
    public static final String EXTRACMD = "extra";

    public static RouteCondition convertHint2RouteCondition(String schemaName, String sql,
                                                            Map<Integer, ParameterContext> parameterSettings) {
        return convertHint2RouteCondition(schemaName, sql, parameterSettings, false);
    }

    public static RouteCondition convertHint2RouteCondition(String schemaName, String sql,
                                                            Map<Integer, ParameterContext> parameterSettings,
                                                            boolean throwException) {
        return extractHint(schemaName, sql, parameterSettings, throwException);
    }

    public static RouteCondition extractHint(String schemaName, String sql,
                                             Map<Integer, ParameterContext> parameterSettings,
                                             boolean throwException) {
        String tddlHint = extractHintString(sql, parameterSettings);
        if (StringUtils.isNotEmpty(tddlHint)) {
            try {
                JSONObject jsonObject = JSON.parseObject(tddlHint);
                String type = jsonObject.getString(TYPE);
                if (throwException && type != null) {
                    throw new NotSupportException("routing hint ");
                }
                if (TYPE_DIRECT.equalsIgnoreCase(type)) {
                    return decodeDirect(schemaName, jsonObject);
                } else if (TYPE_CONDITION.equalsIgnoreCase(type)) {
                    return decodeCondition(jsonObject);
                } else if (TYPE_FULL.equalsIgnoreCase(type)) {
                    return decodeFull(jsonObject);
                } else {
                    return decodeExtra(jsonObject);
                }
            } catch (JSONException e) {
                logger.error("convert tddl hint to RouteCondition failed, check the hint string!", e);
                throw e;
            }

        }

        return null;
    }

    public static RouteCondition convertHintStrToCondition(String schemaName, String tddlHint, boolean throwException) {
        if (StringUtils.isNotEmpty(tddlHint)) {
            try {
                JSONObject jsonObject = JSON.parseObject(tddlHint);
                String type = jsonObject.getString(TYPE);
                if (throwException && type != null) {
                    throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER_ERROR_HINT, "routing hint ");
                }
                if (TYPE_DIRECT.equalsIgnoreCase(type)) {
                    return decodeDirect(schemaName, jsonObject);
                } else if (TYPE_CONDITION.equalsIgnoreCase(type)) {
                    return decodeCondition(jsonObject);
                } else if (TYPE_FULL.equalsIgnoreCase(type)) {
                    return decodeFull(jsonObject);
                } else {
                    return decodeExtra(jsonObject);
                }
            } catch (JSONException e) {
                logger.error("convert tddl hint to RouteCondition failed, check the hint string!", e);
                throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER_ERROR_HINT,
                    "convert hint to RouteCondition failed");
            }

        }
        return null;
    }

    public static RouteCondition decodeDirect(String schemaName, JSONObject jsonObject) {
        DirectlyRouteCondition rc = new DirectlyRouteCondition();
        decodeExtra(rc, jsonObject);
        String tableString = containsKvNotBlank(jsonObject, REALTABS);
        if (tableString != null) {
            JSONArray jsonTables = JSON.parseArray(tableString);
            // 设置table的Set<String>
            if (jsonTables.size() > 0) {
                Set<String> tables = new HashSet<String>(jsonTables.size());
                for (int i = 0; i < jsonTables.size(); i++) {
                    tables.add(jsonTables.getString(i));
                }
                rc.setTables(tables);
                decodeVtab(rc, jsonObject);
            }
        }

        String dbId = containsKvNotBlank(jsonObject, DBID);
        if (dbId == null) {
            throw new RuntimeException("hint contains no property 'dbid'.");
        }

        if (containsKvNotBlank(jsonObject, DBINDEX) != null) {
            List<Group> groups = OptimizerContext.getContext(schemaName).getMatrix().getGroups();
            String[] ids = StringUtils.split(dbId, ',');
            String[] hintIndexs = new String[ids.length];
            int i = 0;
            for (String id : ids) {
                try {
                    int index = Integer.parseInt(id);
                    if (index < groups.size() && index >= 0) {
                        hintIndexs[i++] = groups.get(index).getName();
                    } else { // 如果超过库数量
                        // hintIndexs[i++] = String.valueOf(index);
                        hintIndexs[i++] =
                            OptimizerContext.getContext(schemaName).getRuleManager().getDefaultDbIndex(null);
                    }
                } catch (Throwable e) {
                    hintIndexs[i++] = id;
                }

            }

            dbId = StringUtils.join(hintIndexs, ",");
        }
        rc.setDBId(dbId);
        return rc;
    }

    public static RouteCondition decodeFull(JSONObject jsonObject) {
        FullRouteCondition rc = new FullRouteCondition();
        decodeExtra(rc, jsonObject);
        decodeVtab(rc, jsonObject);
        return rc;
    }

    private static void decodeVtab(RouteCondition rc, JSONObject jsonObject) throws JSONException {
        String virtualTableName = containsKvNotBlank(jsonObject, VTAB);
        /*
         * blank is acceptable if (virtualTableName == null) { throw new
         * SqlParserException("hint contains no property 'vtab'."); }
         */

        rc.setVirtualTableName(virtualTableName);
    }

    private static ExtraCmdRouteCondition decodeExtra(JSONObject jsonObject) throws JSONException {
        ExtraCmdRouteCondition rc = new ExtraCmdRouteCondition();
        String extraCmd = containsKvNotBlank(jsonObject, EXTRACMD);
        if (StringUtils.isNotEmpty(extraCmd)) {
            JSONObject extraCmds = JSON.parseObject(extraCmd);
            for (Map.Entry<String, Object> entry : extraCmds.entrySet()) {
                rc.getExtraCmds().put(entry.getKey(), entry.getValue());
            }
        }
        return rc;
    }

    private static void decodeExtra(RouteCondition rc, JSONObject jsonObject) throws JSONException {
        String extraCmd = containsKvNotBlank(jsonObject, EXTRACMD);
        if (StringUtils.isNotEmpty(extraCmd)) {
            JSONObject extraCmds = JSON.parseObject(extraCmd);
            rc.getExtraCmds().putAll(extraCmds);
        }
    }

    public static RouteCondition decodeCondition(JSONObject jsonObject) {
        RuleRouteCondition sc = new RuleRouteCondition();
        decodeVtab(sc, jsonObject);
        decodeExtra(sc, jsonObject);
        decodeSpecifyInfo(sc, jsonObject);
        String paramsStr = containsKvNotBlank(jsonObject, PARAMS);
        if (paramsStr != null) {
            JSONArray params = JSON.parseArray(paramsStr);
            if (params != null) {
                for (int i = 0; i < params.size(); i++) {
                    JSONObject o = params.getJSONObject(i);
                    JSONArray exprs = o.getJSONArray(EXPR);
                    String paramtype = o.getString(PARAMTYPE);
                    if (o.containsKey(RELATION)) {
                        String relation = o.getString(RELATION);
                        ComparativeBaseList comList = null;
                        if (relation != null && AND.equals(relation)) {
                            comList = new ComparativeAND();
                        } else if (relation != null && OR.equals(relation)) {
                            comList = new ComparativeOR();
                        } else {
                            throw new SqlParserException("multi param but no relation,the hint is:" + sc.toString());
                        }

                        String key = null;
                        for (int j = 0; j < exprs.size(); j++) {
                            Comparative comparative = ComparativeStringAnalyser.decodeComparative(exprs.getString(j),
                                paramtype);
                            comList.addComparative(comparative);

                            String temp = ComparativeStringAnalyser.decodeComparativeKey(exprs.getString(j));
                            if (null == key) {
                                key = temp;
                            } else if (!temp.equals(key)) {
                                throw new SqlParserException(
                                    "decodeCondition not support one relation with multi key,the relation is:["
                                        + relation + "],expr list is:[" + exprs.toString());
                            }
                        }
                        sc.put(key, comList);
                    } else {
                        if (exprs.size() == 1) {
                            String key = ComparativeStringAnalyser.decodeComparativeKey(exprs.getString(0));
                            Comparative comparative = ComparativeStringAnalyser.decodeComparative(exprs.getString(0),
                                paramtype);
                            sc.put(key, comparative);
                        } else {
                            throw new SqlParserException("relation neither 'and' nor 'or',but expr size is not 1");
                        }
                    }
                }
            }
        }

        return sc;
    }

    protected static void decodeSpecifyInfo(RouteCondition condition, JSONObject jsonObject) throws JSONException {
        String skip = containsKvNotBlank(jsonObject, "skip");
        String max = containsKvNotBlank(jsonObject, "max");
        String orderby = containsKvNotBlank(jsonObject, "orderby");
        if (skip != null || max != null || orderby != null) {
            throw new SqlParserException("Don't support this hint");
        }
    }

    /**
     * 从sql中解出hint,并且将hint里面的?替换为参数的String形式
     */
    public static String extractHintString(String sql, Map<Integer, ParameterContext> parameterSettings) {
        String tddlHint = HintParser.getInstance().getTddlHint(ByteString.from(sql));

        if (null == tddlHint || "".endsWith(tddlHint)) {
            return null;
        }

        StringBuffer sb = new StringBuffer();
        int size = tddlHint.length();
        int parameters = 1;
        for (int i = 0; i < size; i++) {
            if (tddlHint.charAt(i) == '?') {
                // TDDLHINT只能设置简单值
                if (parameterSettings == null) {
                    throw new SqlParserException("hint中使用了'?'占位符,却没有设置setParameter()");
                }

                ParameterContext param = parameterSettings.get(parameters);
                if (param == null) {
                    throw new SqlParserException(
                        "Parameter index out of range (" + parameters + " > number of parameters, which is "
                            + parameterSettings.size() + ").");
                }

                Object arg = param.getArgs()[1];
                if ((param.getParameterMethod() == ParameterMethod.setString || arg instanceof String)
                    && !isQuoteSurround(tddlHint, i) && !isAssignSurround(tddlHint, i)) {
                    sb.append('\'');
                    sb.append(arg);
                    sb.append('\'');
                } else {
                    sb.append(arg);
                }
                parameters++;
            } else {
                sb.append(tddlHint.charAt(i));
            }
        }
        return sb.toString();
    }

    private static boolean isQuoteSurround(String tddlHint, int index) {
        for (int i = index - 1; i >= 0; i--) {
            char ch = tddlHint.charAt(i);
            // 找到之前的第一个非空白的字符，判断是否为引号
            if (!Character.isWhitespace(ch)) {
                if (ch == '\'' || ch == '"') {
                    return true;
                } else {
                    break;
                }
            }
        }

        for (int i = index + 1; i < tddlHint.length(); i++) {
            char ch = tddlHint.charAt(i);
            // 找到之后的第一个非空白的字符，判断是否为引号
            if (!Character.isWhitespace(ch)) {
                if (ch == '\'' || ch == '"') {
                    return true;
                } else {
                    break;
                }
            }
        }

        return false;
    }

    private static boolean isAssignSurround(String tddlHint, int index) {
        for (int i = index - 1; i >= 0; i--) {
            char ch = tddlHint.charAt(i);
            // 找到之前的第一个非空白的字符，判断是否为引号
            if (!Character.isWhitespace(ch)) {
                if (ch == '=') {
                    return true;
                } else {
                    break;
                }
            }
        }

        for (int i = index + 1; i < tddlHint.length(); i++) {
            char ch = tddlHint.charAt(i);
            // 找到之后的第一个非空白的字符，判断是否为引号
            if (!Character.isWhitespace(ch)) {
                if (ch == ':') {
                    return true;
                } else {
                    break;
                }
            }
        }

        return false;
    }

    private static String containsKvNotBlank(JSONObject jsonObject, String key) throws JSONException {
        if (!jsonObject.containsKey(key)) {
            return null;
        }

        String value = jsonObject.getString(key);
        if (TStringUtil.isBlank(value)) {
            return null;
        }
        return value;
    }
}
