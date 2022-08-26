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

package com.alibaba.polardbx.rule.impl;

import com.alibaba.polardbx.rule.virtualnode.VirtualNodeMap;
import com.alibaba.polardbx.rule.VirtualTableSupport;

import java.util.Map;

/**
 * @version 1.0
 * @since 1.6
 */
public class DbVirtualNodeRule extends VirtualNodeGroovyRule {

    public DbVirtualNodeRule(VirtualTableSupport tableRule, String expression, VirtualNodeMap vNodeMap,
                             boolean lazyInit) {
        super(tableRule, expression, vNodeMap, lazyInit);
    }

    public DbVirtualNodeRule(VirtualTableSupport tableRule, String expression, VirtualNodeMap vNodeMap,
                             String extraPackagesStr, boolean lazyInit) {
        super(tableRule, expression, vNodeMap, extraPackagesStr, lazyInit);
    }

    public String eval(Map<String, Object> columnValues, Object outerContext) {
        return eval(columnValues, outerContext, null);
    }

    public String eval(Map<String, Object> columnValues, Object outerContext, Map<String, Object> calcParams) {
        String key = (String) columnValues.get(REAL_TABLE_NAME_KEY);
        return super.map(key);
    }
}
