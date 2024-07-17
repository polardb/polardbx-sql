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

package com.alibaba.polardbx.gms.lbac;

import com.alibaba.polardbx.gms.lbac.component.ComponentInstance;
import com.alibaba.polardbx.gms.lbac.component.LBACSecurityLabelComponent;

/**
 * @author pangzhaoxing
 */
public interface LBACRules {

    /**
     * 判断在read场景下，c1是否大于c2
     *
     * @param component c1、c2同属的SecurityLabelComponent
     */
    boolean canRead(LBACSecurityLabelComponent component, ComponentInstance c1, ComponentInstance c2);

    /**
     * 判断在write场景下，c1是否大于c2
     *
     * @param component c1、c2同属的SecurityLabelComponent
     */
    boolean canWrite(LBACSecurityLabelComponent component, ComponentInstance c1, ComponentInstance c2);

}
