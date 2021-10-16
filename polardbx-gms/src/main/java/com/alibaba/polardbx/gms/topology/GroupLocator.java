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

package com.alibaba.polardbx.gms.topology;

import java.util.List;
import java.util.Map;

/**
 * Interface for compute which group should locate on which storage instance
 *
 * @author chenghui.lch
 */
public interface GroupLocator {

    /**
     * Build group location for normal group and  single group
     */
    void buildGroupLocationInfo(Map<String, List<String>> normalGroupMap,
                                Map<String, List<String>> singleGroupMap);

}
