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

package com.alibaba.polardbx.gms.lbac.component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author pangzhaoxing
 */
public class ArraySecurityLabelComponent extends LBACSecurityLabelComponent {

    protected final Map<String, Integer> tagIndexMap = new HashMap<>();

    public ArraySecurityLabelComponent(String componentName, List<String> tags) {
        super(componentName, ComponentType.ARRAY);
        for (int i = 0; i < tags.size(); i++) {
            if (tagIndexMap.put(tags.get(i), i) != null) {
                throw new IllegalArgumentException("the tags should not be same");
            }
        }
    }

    public ArraySecurityLabelComponent(String componentName, String componentContent) {
        super(componentName, ComponentType.SET);
    }

    @Override
    public Set<String> getAllTags() {
        return tagIndexMap.keySet();
    }

    /**
     * @param tag negative means tag is not in array
     */
    public int getTagIndex(String tag) {
        return tagIndexMap.getOrDefault(tag, -1);
    }

    @Override
    public boolean containTag(String tag) {
        return tagIndexMap.containsKey(tag);
    }
}
