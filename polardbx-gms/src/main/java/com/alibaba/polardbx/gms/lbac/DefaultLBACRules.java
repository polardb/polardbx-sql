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

import com.alibaba.polardbx.gms.lbac.component.ArraySecurityLabelComponent;
import com.alibaba.polardbx.gms.lbac.component.ComponentInstance;
import com.alibaba.polardbx.gms.lbac.component.LBACSecurityLabelComponent;
import com.alibaba.polardbx.gms.lbac.component.SetSecurityLabelComponent;
import com.alibaba.polardbx.gms.lbac.component.TreeSecurityLabelComponent;
import com.alibaba.polardbx.lbac.LBACException;

import java.util.Set;

/**
 * @author pangzhaoxing
 */
public class DefaultLBACRules implements LBACRules {
    private boolean canRead(SetSecurityLabelComponent component,
                            ComponentInstance c1, ComponentInstance c2) {
        Set<String> tags1 = c1.getTags();
        Set<String> tags2 = c2.getTags();
        if (tags2.isEmpty()) {
            return true;
        }
        if (tags1.isEmpty()) {
            return false;
        }
        for (String tag : tags2) {
            if (!component.containTag(tag)) {
                throw new LBACException("security label tag is not belong to component");
            }
            if (!tags1.contains(tag)) {
                return false;
            }
        }
        return true;
    }

    private boolean canRead(TreeSecurityLabelComponent component,
                            ComponentInstance c1, ComponentInstance c2) {
        Set<String> tags1 = c1.getTags();
        Set<String> tags2 = c2.getTags();
        if (tags2.isEmpty()) {
            return true;
        }
        if (tags1.isEmpty()) {
            return false;
        }
        for (String tag1 : tags1) {
            for (String tag2 : tags2) {
                TreeSecurityLabelComponent.Node node1 = component.getNode(tag1);
                TreeSecurityLabelComponent.Node node2 = component.getNode(tag2);
                if (node1 == null || node2 == null) {
                    throw new LBACException("security label tag is not belong to component");
                }
                if (node1 == node2) {
                    return true;
                }
                if (TreeSecurityLabelComponent.Node.isAncestor(node1, node2)) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean canRead(ArraySecurityLabelComponent component,
                            ComponentInstance c1, ComponentInstance c2) {

        String tags1 = c1.getTags().iterator().next();
        String tags2 = c2.getTags().iterator().next();
        if (tags2.isEmpty()) {
            return true;
        }
        if (tags1.isEmpty()) {
            return false;
        }
        int index1 = component.getTagIndex(tags1);
        int index2 = component.getTagIndex(tags2);
        if (index1 < 0 || index2 < 0) {
            throw new LBACException("security label tag is not belong to component");
        }
        return index1 <= index2;
    }

    private boolean canWrite(SetSecurityLabelComponent component,
                             ComponentInstance c1, ComponentInstance c2) {
        return canRead(component, c1, c2);
    }

    private boolean canWrite(TreeSecurityLabelComponent component,
                             ComponentInstance c1, ComponentInstance c2) {
        return canRead(component, c1, c2);
    }

    private boolean canWrite(ArraySecurityLabelComponent component,
                             ComponentInstance c1, ComponentInstance c2) {
        return canRead(component, c1, c2);
    }

    @Override
    public boolean canRead(LBACSecurityLabelComponent component, ComponentInstance c1, ComponentInstance c2) {

        switch (component.getType()) {
        case TREE:
            return canRead((TreeSecurityLabelComponent) component, c1, c2);
        case ARRAY:
            return canRead((ArraySecurityLabelComponent) component, c1, c2);
        case SET:
            return canRead((SetSecurityLabelComponent) component, c1, c2);
        default:
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public boolean canWrite(LBACSecurityLabelComponent component, ComponentInstance c1, ComponentInstance c2) {
        switch (component.getType()) {
        case TREE:
            return canWrite((TreeSecurityLabelComponent) component, c1, c2);
        case ARRAY:
            return canWrite((ArraySecurityLabelComponent) component, c1, c2);
        case SET:
            return canWrite((SetSecurityLabelComponent) component, c1, c2);
        default:
            throw new UnsupportedOperationException();
        }
    }
}
