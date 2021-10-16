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

import com.alibaba.polardbx.common.model.lifecycle.Lifecycle;
import com.alibaba.polardbx.common.model.sqljep.ComparativeMapChoicer;
import com.alibaba.polardbx.rule.exception.RouteCompareDiffException;
import com.alibaba.polardbx.rule.model.MatcherResult;

import java.util.List;
import java.util.Map;

/**
 * 基于tddl管理体系的table rule实现
 *
 * @author jianghang 2013-11-5 上午11:04:35
 * @since 5.0.0
 */
public interface TddlTableRule extends Lifecycle {

    /**
     * 根据当前规则，计算table rule结果. <br/>
     * ps. 当前规则可能为local本地规则或者是当前使用中的远程规则
     *
     * @param vtab 逻辑表名
     * @param choicer 参数提取，比如statement sql中自带的参数
     * @param args 通过prepareStatement设置的参数
     */
    public MatcherResult route(String vtab, ComparativeMapChoicer choicer, List<Object> args);

    /**
     * 指定version规则版本，计算table rule结果
     *
     * @param vtab 逻辑表名
     * @param choicer 参数提取，比如statement sql中自带的参数
     * @param args 通过prepareStatement设置的参数
     * @param specifyVtr 指定规则
     */
    public MatcherResult route(String vtab, ComparativeMapChoicer choicer, List<Object> args, String version);

    /**
     * 指定规则，计算table rule结果
     *
     * @param vtab 逻辑表名
     * @param choicer 参数提取，比如statement sql中自带的参数
     * @param args 通过prepareStatement设置的参数
     * @param specifyVtr 指定规则
     */
    public MatcherResult route(String vtab, ComparativeMapChoicer choicer, List<Object> args,
                               VirtualTableRoot specifyVtr);

    public MatcherResult routeMverAndCompare(boolean isSelect, String vtab, ComparativeMapChoicer choicer,
                                             List<Object> args) throws RouteCompareDiffException;

    /**
     * 根据当前规则，同时支持新旧规则模式的计算 <br/>
     * ps. 运行时切库会同时指定新旧两个版本号，针对读请求使用老规则，针对写请求如果新老规则相同则正常返回，如果不同则抛diff异常.<br/>
     *
     * <pre>
     * 一般切换步骤：
     * 1. 根据新规则，将老库数据做一次数据复制
     * 2. 线上配置新老规则，同时生效
     * 3. 应用前端停写，等待后端增量数据迁移完全追平
     * 4. 线上配置为新规则
     * 5. 删除老库上的数据
     * </pre>
     *
     * @param sqlType 原始sql类型
     * @param vtab 逻辑表名
     * @param choicer 参数提取，比如statement sql中自带的参数
     * @param args 通过prepareStatement设置的参数
     * @param forceAllowFullTableScan 允许用户强制指定是否开启全表扫描,true代表强制开启/false代表使用当前规则
     */
    public MatcherResult routeMverAndCompare(boolean isSelect, String vtab, ComparativeMapChoicer choicer,
                                             List<Object> args, boolean forceAllowFullTableScan,
                                             List<TableRule> ruleList) throws RouteCompareDiffException;

    /**
     * 根据当前规则，同时支持新旧规则模式的计算 <br/>
     * ps. 运行时切库会同时指定新旧两个版本号，针对读请求使用老规则，针对写请求如果新老规则相同则正常返回，如果不同则抛diff异常.<br/>
     *
     * <pre>
     * 一般切换步骤：
     * 1. 根据新规则，将老库数据做一次数据复制
     * 2. 线上配置新老规则，同时生效
     * 3. 应用前端停写，等待后端增量数据迁移完全追平
     * 4. 线上配置为新规则
     * 5. 删除老库上的数据
     * </pre>
     *
     * @param sqlType 原始sql类型
     * @param vtab 逻辑表名
     * @param choicer 参数提取，比如statement sql中自带的参数
     * @param args 通过prepareStatement设置的参数
     * @param forceAllowFullTableScan 允许用户强制指定是否开启全表扫描,true代表强制开启/false代表使用当前规则
     */
    public MatcherResult routeMverAndCompare(boolean isSelect, String vtab, ComparativeMapChoicer choicer,
                                             List<Object> args, boolean forceAllowFullTableScan,
                                             List<TableRule> ruleList,
                                             Map<String, Object> calcParams) throws RouteCompareDiffException;

    // ==================== 以下方法可支持非jdbc协议使用rule =================

    /**
     * 根据当前规则，计算table rule结果. <br/>
     *
     * @param vtab 逻辑表
     * @param condition 类似statement sql表达式
     */
    public MatcherResult route(String vtab, String condition);

    /**
     * 指定规则，计算table rule结果. <br/>
     *
     * @param vtab 逻辑表
     * @param condition 类似statement sql表达式
     */
    public MatcherResult route(String vtab, String condition, VirtualTableRoot specifyVtr);

    /**
     * 指定version规则版本，计算table rule结果. <br/>
     *
     * @param vtab 逻辑表
     * @param condition 类似statement sql表达式
     */
    public MatcherResult route(String vtab, String condition, String version);

}
