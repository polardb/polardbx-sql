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

import org.apache.commons.lang.StringUtils;

/**
 * 做一下老的rule兼容处理，tddl5版本类名做了下统一调整，所以以前配置中使用的一下类名已经不存在，需要做转换
 *
 * @since 5.0.0
 */
public class RuleCompatibleHelper {
    public static final String RULE_CURRENT_SHARD_FUNCTION_META_NAME = "com.alibaba.polardbx.rule.meta";

    public static final String RULE_TDDL_SHARD_FUNCTION_META_NAME = "com.taobao.tddl.rule.meta";

    public static String compatibleShardFunctionMeta(String ruleStr) {
        ruleStr =
            StringUtils.replace(ruleStr, RULE_TDDL_SHARD_FUNCTION_META_NAME, RULE_CURRENT_SHARD_FUNCTION_META_NAME);
        return ruleStr;
    }

    public static String forwardCompatibleShardFunctionMeta(String ruleStr) {
        return StringUtils.replace(ruleStr, RULE_CURRENT_SHARD_FUNCTION_META_NAME, RULE_TDDL_SHARD_FUNCTION_META_NAME);
    }
}
