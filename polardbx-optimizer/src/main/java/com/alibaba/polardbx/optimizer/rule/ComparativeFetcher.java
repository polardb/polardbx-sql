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

package com.alibaba.polardbx.optimizer.rule;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.jdbc.RawString;
import com.alibaba.polardbx.common.model.sqljep.Comparative;
import com.alibaba.polardbx.common.model.sqljep.ComparativeAND;
import com.alibaba.polardbx.common.model.sqljep.ComparativeBaseList;
import com.alibaba.polardbx.common.model.sqljep.ComparativeOR;
import com.alibaba.polardbx.common.model.sqljep.DynamicComparative;
import com.alibaba.polardbx.common.model.sqljep.ExtComparative;
import com.alibaba.polardbx.common.utils.timezone.InternalTimeZone;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.TimestampType;
import com.alibaba.polardbx.optimizer.core.expression.calc.IExpression;
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import com.alibaba.polardbx.optimizer.utils.SubQueryDynamicParamUtils;
import com.alibaba.polardbx.rule.TableRule;
import com.alibaba.polardbx.rule.utils.CalcParamsAttribute;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexNode;
import org.apache.commons.collections.MapUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.function.Function;

/**
 * @author chenghui.lch
 */
public class ComparativeFetcher {

    private InternalTimeZone shardRouterTimeZone;

    public ComparativeFetcher(InternalTimeZone shardRouterTimeZone) {
        this.shardRouterTimeZone = shardRouterTimeZone;
    }

    /**
     * Copy and get comparatives from comparatives and then replace the dynamic params to value obj
     */
    public Comparative getComparativeAndReplaceParams(TableRule tableRule,
                                                      Map<String, Comparative> comparatives,
                                                      String colName,
                                                      Map<Integer, ParameterContext> param,
                                                      Map<String, DataType> dataTypeMap,
                                                      Map<String, Object> calcParams) {

        /**
         *  filter中col与val的动态参数idx的map
         */
        Map<String, Integer> condColValIdxMap =
            (Map<String, Integer>) calcParams.get(CalcParamsAttribute.COND_COL_IDX_MAP);

        if (condColValIdxMap != null) {
            // 如果指定了 filter中col与val的动态参数idx的映射关系，直接使用
            // 通常简单的等值点查会有传这个参数

            /**
             * 没有参数
             */
            if (MapUtils.isEmpty(param)) {
                return null;
            }

            int index = condColValIdxMap.get(colName);
            Object paramVal = param.get(index + 1).getValue();
            DataType dataType = dataTypeMap.get(colName);
            // Only TIMESTAMP/DATETIME type need correct timezone.
            if (dataType instanceof TimestampType) {
                paramVal = correctTimeZoneForParamVal(tableRule, colName, dataType, calcParams, paramVal);
            }
            return new Comparative(Comparative.Equivalent, dataType.convertJavaFrom(paramVal));
        }

        if (MapUtils.isEmpty(comparatives)) {
            return null;
        }

        /**
         * 没有参数
         */
        if (MapUtils.isEmpty(param)) {
            Comparative c = findComparativeIgnoreCase(comparatives, colName);
            if (c == null) {
                return null;
            }
            Comparative clone = c.clone();
            Object paramVal = c.getValue();
            if (paramVal instanceof RexDynamicParam) {

                if (SubQueryDynamicParamUtils.isScalarSubQueryConstant((RexDynamicParam) paramVal)) {
                    /**
                     * When Sql Parameterization is closed (e.g. in PlanTestCommon's testcases of UnitTest scene), the param of ExecutionContext will
                     * be empty, but the scalarSubQuery of sql will be replaced as RexDynamicParam,
                     * so come here ignore the scalar subquery predicate pruning and return null
                     */
                    ExecutionContext ec = (ExecutionContext) calcParams.get(CalcParamsAttribute.EXECUTION_CONTEXT);
                    Object[] valResult = new Object[1];
                    boolean isSucc =
                        SubQueryDynamicParamUtils.tryExecAndFetchScalarSubQueryConstantValue((RexDynamicParam) paramVal,
                            ec, valResult);
                    if (!isSucc) {
                        return null;
                    }
                    if (valResult[0] == null && clone.getComparison() != Comparative.Equivalent) {
                        return null;
                    }
                    paramVal = valResult[0];

                } else {
                    throw new IllegalArgumentException(
                        "RexDynamicParam should not be enter here, might cause by params missing.");
                }
            }
            if (paramVal != null) {
                DataType dataType = dataTypeMap.get(colName);
                // Only TIMESTAMP/DATETIME type need correct timezone.
                if (dataType instanceof TimestampType) {
                    paramVal = correctTimeZoneForParamVal(tableRule, colName, dataType, calcParams, paramVal);
                }
                clone.setValue(dataType.convertJavaFrom(paramVal));
            } else {
                clone.setValue(null);
            }

            return clone;
        } else {
            /**
             * 用实际值替换参数
             */
            final Comparative c = findComparativeIgnoreCase(comparatives, colName);
            if (c != null) {
                Comparative clone = c.clone();
                Comparative newCompAfterReplacedParams =
                    replaceParamWithValue(tableRule, colName, clone, param, dataTypeMap, colName, calcParams);
                if (newCompAfterReplacedParams != null) {
                    return newCompAfterReplacedParams;
                } else {
                    return null;
                }
            } else {
                return null;
            }
        }
    }

    private Object correctTimeZoneForParamVal(TableRule tableRule, String colName,
                                              DataType dataType,
                                              Map<String, Object> calcParams,
                                              Object paramVal) {
        InternalTimeZone connTimeZoneInfo = (InternalTimeZone) calcParams.get(CalcParamsAttribute.CONN_TIME_ZONE);
        TimeZone connTimeZone = null;
        if (connTimeZoneInfo != null) {
            connTimeZone = connTimeZoneInfo.getTimeZone();
        }

        TimeZoneCorrector timeZoneCorrector = new TimeZoneCorrector(shardRouterTimeZone, tableRule, connTimeZone);
        paramVal = timeZoneCorrector.correctTimeZoneIfNeed(colName, dataType, paramVal, calcParams);
        Object finalParamVal = dataType.convertJavaFrom(paramVal);
        return finalParamVal;
    }

    private static Comparative findComparativeIgnoreCase(Map<String, Comparative> comparatives, String colName) {
        for (Map.Entry<String, Comparative> entry : comparatives.entrySet()) {
            if (entry.getKey().equalsIgnoreCase(colName)) {
                return entry.getValue();
            }
        }
        return null;
    }

    private Comparative replaceParamWithValue(TableRule tableRule, String colName,
                                              Comparative comparative,
                                              Map<Integer, ParameterContext> param,
                                              DataType dataType, Map<String, Object> calcParams) {
        Object v = comparative.getValue();
        Object paramVal = null;
        Comparative newComp = comparative;
        boolean isSucc = true;
        if (v instanceof RexDynamicParam) {
            int index = ((RexDynamicParam) v).getIndex();
            if (index != PlannerUtils.SCALAR_SUBQUERY_PARAM_INDEX && index != PlannerUtils.APPLY_SUBQUERY_PARAM_INDEX) {
                paramVal = param.get(index + 1).getValue();
            } else if (index == PlannerUtils.SCALAR_SUBQUERY_PARAM_INDEX) {
                ExecutionContext ec = (ExecutionContext) calcParams.get(CalcParamsAttribute.EXECUTION_CONTEXT);
                Object[] val = new Object[1];
                if (SubQueryDynamicParamUtils.fetchScalarSubQueryConstantValue(v, ec.getScalarSubqueryCtxMap(), true,
                    val)) {
                    paramVal = val[0];
                    if (paramVal == null && comparative.getComparison() != Comparative.Equivalent) {
                        isSucc = false;
                    }
                } else {
                    isSucc = false;
                }
            } else {
                return comparative;
            }

            if (!isSucc) {
                return null;
            }
        } else if (v instanceof RawString) {
            paramVal = ((RawString) v).getObj(comparative.getRawIndex(), comparative.getSkIndex());
        } else if (v instanceof RexNode) {
            // Construct a temporary ExecutionContext to wrap parameters
            ExecutionContext context = new ExecutionContext();
            Parameters parameters = new Parameters();
            parameters.setParams(param);
            context.setParams(parameters);
            context.setTimeZone(shardRouterTimeZone);

            // Eval with null row (must be constant here)
            IExpression expression = RexUtils.buildRexNode((RexNode) v, context);
            paramVal = expression.eval(null);
        } else {
            /*
             *  comparative.getValue() may be a Java Object ( such String/Date/Timestamp, ....)
             *
             *  e.g.
             *  for the insert sql (  check_date is timestamp, check_date is shard key ):
             *  insert into tb (id, check_date, is_freeze) values (1, '2019-12-12 23:00',1)
             *  ,
             *  this sql will be constructed a comparative of check_date='2019-12-12 23:00',
             *  not a comparative of check_date=?.
             *
             *  so comparative.getValue() maybe occur a non-RexDynamicParam value
             */
            paramVal = v;
        }

        if (paramVal != null) {
            // Only TIMESTAMP/DATETIME type need correct timezone.
            if (dataType instanceof TimestampType) {
                paramVal = correctTimeZoneForParamVal(tableRule, colName, dataType, calcParams, paramVal);
            }
            comparative.setValue(dataType.convertJavaFrom(paramVal));
        } else {
            comparative.setValue(null);
        }
        return comparative;
    }

    private Comparative replaceParamWithValue(TableRule tableRule,
                                              String colName,
                                              Comparative comparative/*comparative has been a clone*/,
                                              Map<Integer, ParameterContext> param,
                                              Map<String, DataType> dataTypeMap,
                                              String name,
                                              Map<String, Object> calcParams) {
        if (comparative instanceof ComparativeAND || comparative instanceof ComparativeOR) {
            List<Comparative> compList = ((ComparativeBaseList) comparative).getList();
            List<Comparative> newCompList = new ArrayList<>();
            boolean isCompOr = comparative instanceof ComparativeOR;
            Comparative finalComp = comparative;
            boolean useDynamicComparative = ((ComparativeBaseList) comparative).getList().size() > 0
                && ((ComparativeBaseList) comparative).getList().get(0) instanceof DynamicComparative;
            if (useDynamicComparative) {
                Comparative t = ((ComparativeBaseList) comparative).getList().get(0);
                Object v = t.getValue();
                if (v instanceof RexDynamicParam) {
                    int index = ((RexDynamicParam) v).getIndex();
                    DataType dataType = dataTypeMap.get(name);
                    Object obj = param.get(index + 1).getValue();
                    if (obj instanceof RawString) {
                        RawString rawString = ((RawString) obj).convertType(o -> {
                            if (dataType instanceof TimestampType) {
                                return correctTimeZoneForParamVal(tableRule, colName, dataType, calcParams, o);
                            }
                            return dataType.convertJavaFrom(o);
                        }, t.getSkIndex());
                        for (int i = 0; i < rawString.size(); i++) {
                            Comparative temp = new Comparative(Comparative.Equivalent, rawString);
                            temp.setRawIndex(i, t.getSkIndex());
                            ((ComparativeBaseList) comparative).addComparative(temp);
                        }
                    } else {
                        Comparative temp = new Comparative(Comparative.Equivalent, obj);
                        ((ComparativeBaseList) comparative).addComparative(temp);
                    }
                }
                ((ComparativeBaseList) comparative).getList().remove(t);
                return comparative;
            } else {
                for (int i = 0; i < compList.size(); i++) {
                    Comparative c = compList.get(i);
                    Comparative newC = null;
                    if (c instanceof ComparativeAND || c instanceof ComparativeOR) {
                        newC = replaceParamWithValue(tableRule, colName, c, param, dataTypeMap, name, calcParams);
                    } else if (c instanceof ExtComparative) {
                        newC = replaceParamWithValue(tableRule,
                            colName,
                            c,
                            param,
                            dataTypeMap.get(((ExtComparative) c).getColumnName()),
                            calcParams);
                    } else {
                        newC = replaceParamWithValue(tableRule, colName, c, param, dataTypeMap.get(name), calcParams);
                    }

                    if (isCompOr) {
                        if (newC == null) {
                            /**
                             * When current comparative is a OR-comp, any comparative is failed to replace params,
                             * it should go to full-scan instead.
                             */
                            finalComp = null;
                            break;
                        }
                    } else {
                        if (newC != null) {
                            newCompList.add(newC);
                        }
                    }
                }

                if (isCompOr) {
                    return finalComp;
                } else {
                    if (newCompList.size() > 1) {
                        ((ComparativeAND) finalComp).setList(newCompList);
                    } else if (newCompList.size() == 1) {
                        finalComp = newCompList.get(0);
                    } else {
                        finalComp = null;
                    }
                    return finalComp;
                }
            }

        } else if (comparative instanceof ExtComparative) {
            return replaceParamWithValue(tableRule,
                colName,
                comparative,
                param,
                dataTypeMap.get(((ExtComparative) comparative).getColumnName()),
                calcParams);
        } else {
            return replaceParamWithValue(tableRule, colName, comparative, param, dataTypeMap.get(name), calcParams);
        }
    }
}
