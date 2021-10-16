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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.alibaba.polardbx.rule.TableRule;

/**
 *
 */
public class GroovyRuleShardFuncFinder {

    public static List<String> groovyDateMethodFunctionList = new ArrayList<String>();

    public static Set<String> groovyDateMethodFunctionSet = new HashSet<String>();

    static {

        /**
         * 这里的列表的顺序很重要, 因为第1优先级的列表中可能会出现第2优先级列表的值
         */

        // 第1优先级搜索列表
        groovyDateMethodFunctionList.add(GroovyRuleConstant.YYYY_MM_I_OPT_METHOD);
        groovyDateMethodFunctionList.add(GroovyRuleConstant.YYYY_WEEK_I_OPT_METHOD);
        groovyDateMethodFunctionList.add(GroovyRuleConstant.YYYY_DD_I_OPT_METHOD);
        groovyDateMethodFunctionList.add(GroovyRuleConstant.YYYY_MM_I_METHOD);
        groovyDateMethodFunctionList.add(GroovyRuleConstant.YYYY_WEEK_I_METHOD);
        groovyDateMethodFunctionList.add(GroovyRuleConstant.YYYY_DD_I_METHOD);
        groovyDateMethodFunctionList.add(GroovyRuleConstant.YYYY_I_METHOD);
        groovyDateMethodFunctionList.add(GroovyRuleConstant.MM_DD_I_METHOD);

        // 第2优先级搜索列表
        groovyDateMethodFunctionList.add(GroovyRuleConstant.MM_I_METHOD);
        groovyDateMethodFunctionList.add(GroovyRuleConstant.WEEK_METHOD);
        groovyDateMethodFunctionList.add(GroovyRuleConstant.DD_I_METHOD);

    }

    static {
        groovyDateMethodFunctionSet.add(GroovyRuleConstant.YYYY_MM_I_OPT_METHOD);
        groovyDateMethodFunctionSet.add(GroovyRuleConstant.YYYY_WEEK_I_OPT_METHOD);
        groovyDateMethodFunctionSet.add(GroovyRuleConstant.YYYY_DD_I_OPT_METHOD);
        groovyDateMethodFunctionSet.add(GroovyRuleConstant.YYYY_MM_I_METHOD);
        groovyDateMethodFunctionSet.add(GroovyRuleConstant.YYYY_WEEK_I_METHOD);
        groovyDateMethodFunctionSet.add(GroovyRuleConstant.YYYY_DD_I_METHOD);
        groovyDateMethodFunctionSet.add(GroovyRuleConstant.YYYY_I_METHOD);
        groovyDateMethodFunctionSet.add(GroovyRuleConstant.MM_DD_I_METHOD);
        groovyDateMethodFunctionSet.add(GroovyRuleConstant.MM_I_METHOD);
        groovyDateMethodFunctionSet.add(GroovyRuleConstant.DD_I_METHOD);
        groovyDateMethodFunctionList.add(GroovyRuleConstant.WEEK_METHOD);
    }

    public static void process(TableRule currentTableRule) {

        if (currentTableRule.isSimple()) {
            return;
        }

        if (currentTableRule.getDbShardFunctionMeta() == null) {

            String[] dbRuleStrs = currentTableRule.getDbRuleStrs();
            if (dbRuleStrs != null && dbRuleStrs.length == 1) {
                for (int i = 0; i < groovyDateMethodFunctionList.size(); ++i) {
                    String groovyFunName = groovyDateMethodFunctionList.get(i);
                    if (dbRuleStrs[0].contains(groovyFunName)) {
                        currentTableRule.setDbGroovyShardMethodName(groovyFunName);
                        break;
                    }
                }
            }
        }

        if (currentTableRule.getTbShardFunctionMeta() == null) {

            String[] tbRuleStrs = currentTableRule.getTbRulesStrs();
            if (tbRuleStrs != null && tbRuleStrs.length == 1) {
                for (int i = 0; i < groovyDateMethodFunctionList.size(); ++i) {
                    String groovyFunName = groovyDateMethodFunctionList.get(i);
                    if (tbRuleStrs[0].contains(groovyFunName)) {
                        currentTableRule.setTbGroovyShardMethodName(groovyFunName);
                        break;
                    }

                }
            }

        }

    }

}
