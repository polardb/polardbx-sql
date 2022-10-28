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

package com.alibaba.polardbx.rule;

import com.alibaba.polardbx.common.model.sqljep.Comparative;
import com.alibaba.polardbx.rule.enums.RuleType;

import java.util.Map;
import java.util.Set;

/**
 * <pre>
 * 若分库有两条规则：
 * 规则一：columnA、columnB?，若columnB没有，则取columnB的所有值域（由描点信息获得）全表扫描
 * 规则二：columnA、columnC。
 * 若sql只包含columnA，则走规则一
 * 若sql只包含columnA、columnC，则走规则二
 *
 * 顺序+优先最大匹配，先匹配所有列，找不到再按去除可选列之后匹配
 * </pre>
 *
 * @author hongxi.chx
 */
public interface ExtRule<T> extends Rule<T> {
    /**
     * 列值对进行rule表达式求值，此时计算需要传入rule type，以处理不同类型rule结果表现的差异
     *
     * @param columnValues 列值对。个数与getRuleColumns相同。
     * @param outerContext 动态的额外参数。比如从ThreadLocal中传入的表名前缀
     * @param calcParams 规则动态计算路由时所需要从外部传入的参数（例如指定是否按需要冷热数据的拓扑进行枚举），
     * 它的作用级别是单条SQL的路由计算，即每次计算规则时都要传一次，从而保证不同路由计算不会相互干扰
     * 它与outerContext的不同是，outerContext的作用域是表级别的，所以同一表的不同路由的参数是共用的
     * @return 根据一组列值对计算结果
     */
    public T eval(Map<String/* 列名 */, Object/* 列值 */> columnValues, Object outerContext,
                  Map<String, Object> calcParams, RuleType ruleType);

    /**
     * 比较树匹配
     *
     * <pre>
     * &#64;param sqlArgs 从SQL提取出来的比较树
     *
     * <pre>
     * getRuleColumns包含的必选列（optional=false）必须在sqlArgs里面有。可选列可以没有
     *  key： String列名
     *  value: sql中按该列提取出的比较树Comparative，已经绑定了参数
     * </pre>
     *
     * @param ctx 规则执行的上下文。用于关联规则执行时，规则间必要信息的传递。对于EnumerativeRule来说。在库表规则有公共列时，
     * 会在每一个库规则的值下面，执行表规则；执行时库规则产生该值的描点信息将以该参数传入。
     * @param outerContext 动态的额外参数。比如从ThreadLocal中传入的表名前缀
     * @param calcParams 规则动态计算路由时所需要从外部传入的参数（例如指定是否按需要冷热数据的拓扑进行枚举），
     * 它的作用级别是单条SQL的路由计算，即每次计算规则时都要传一次，从而保证不同路由计算不会相互干扰
     * 它与outerContext的不同是，outerContext的作用域是表级别的，所以同一表的不同路由的参数是共用的
     * @return 规则计算结果，和得到这个结果的所有数据。
     * </pre>
     */
    public Map<T, ? extends Object> calculate(Map<String/* 列名 */, Comparative> sqlArgs, Object ctx, Object outerContext,
                                              Map<String, Object> calcParams,
                                              RuleType ruleType);

    /**
     * 不返回每个结果对应的得到该结果的输入值（描点）集合
     */
    public Set<T> calculateNoTrace(
        Map<String/* 列名 */, Comparative/* 比较树 */> sqlArgs, Object ctx, Object outerContext, RuleType ruleType);

    /**
     * 不返回每个结果对应的得到该结果的输入值（描点）集合
     *
     * @param calcParams 规则动态计算路由时所需要从外部传入的参数（例如指定是否按需要冷热数据的拓扑进行枚举），
     * 它的作用级别是单条SQL的路由计算，即每次计算规则时都要传一次，从而保证不同路由计算不会相互干扰
     * 它与outerContext的不同是，outerContext的作用域是表级别的，所以同一表的不同路由的参数是共用的
     */
    public Set<T> calculateNoTrace(Map<String/* 列名 */, Comparative/* 比较树 */> sqlArgs, Object ctx, Object outerContext,
                                   Map<String, Object> calcParams,
                                   RuleType ruleType);

    public VirtualTableSupport getTableRule();

}
