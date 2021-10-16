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

/**
 * @author <a href="junyu@taobao.com">junyu</a>
 * @version 1.0
 * @since 1.6
 */
public abstract class VirtualNodeGroovyRule extends GroovyRule<String> {

    protected final VirtualNodeMap vNodeMap;

    public VirtualNodeGroovyRule(VirtualTableSupport tableRule, String expression, VirtualNodeMap vNodeMap,
                                 boolean lazyInit) {
        super(tableRule, expression, lazyInit);
        this.vNodeMap = vNodeMap;
    }

    public VirtualNodeGroovyRule(VirtualTableSupport tableRule, String expression, VirtualNodeMap vNodeMap,
                                 String extraPackagesStr, boolean lazyInit) {
        super(tableRule, expression, extraPackagesStr, lazyInit);
        this.vNodeMap = vNodeMap;
    }

    protected String map(String key) {
        return this.vNodeMap.getValue(key);
    }
}
