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

package com.alibaba.polardbx.common.model.privilege;

import com.taobao.tddl.common.privilege.GrantParameter;
import com.taobao.tddl.common.privilege.PrivilegeLevel;
import com.taobao.tddl.common.privilege.PrivilegePoint;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Set;
import java.util.SortedSet;

@Data
@NoArgsConstructor
public class Privs {
    boolean insertPriv;

    boolean updatePriv;

    boolean deletePriv;

    boolean selectPriv;

    boolean createPriv;

    boolean dropPriv;

    boolean alterPriv;

    boolean indexPriv;

    boolean grantPriv;

    public Privs(boolean insertPriv, boolean updatePriv, boolean deletePriv, boolean selectPriv,
                 boolean createPriv,
                 boolean dropPriv, boolean alterPriv, boolean indexPriv, boolean grantPriv) {
        this.insertPriv = insertPriv;
        this.updatePriv = updatePriv;
        this.deletePriv = deletePriv;
        this.selectPriv = selectPriv;
        this.createPriv = createPriv;
        this.dropPriv = dropPriv;
        this.alterPriv = alterPriv;
        this.indexPriv = indexPriv;
        this.grantPriv = grantPriv;
    }

    public boolean hasNoPrivilege() {
        return !(this.isAlterPriv() || this.isCreatePriv() || this.isCreatePriv()
            || this.isDropPriv() || this.isIndexPriv() || this.isSelectPriv()
            || this.isInsertPriv() || this.isUpdatePriv() || this.isDeletePriv() || this.isGrantPriv());
    }

    public void removePrivilege(Set<PrivilegePoint> toBeRemovedPrivs) {
        toBeRemovedPrivs.stream().forEach(privilegePoint -> {
            switch (privilegePoint) {
            case CREATE:
                setCreatePriv(false);
                break;
            case DROP:
                setDropPriv(false);
                break;
            case ALTER:
                setAlterPriv(false);
                break;
            case INDEX:
                setIndexPriv(false);
                break;
            case INSERT:
                setInsertPriv(false);
                break;
            case SELECT:
                setSelectPriv(false);
                break;
            case UPDATE:
                setUpdatePriv(false);
                break;
            case DELETE:
                setDeletePriv(false);
                break;
            default:
                break;
            }
        });
    }

    public GrantParameter toGrantParameter(String instId) {
        GrantParameter grantParam = new GrantParameter();
        grantParam.setWithGrantOption(this.grantPriv);
        SortedSet<PrivilegePoint> sortedSet = Sets.newTreeSet();
        sortedSet.addAll(generatePrivilegePoints());
        grantParam.setPrivs(sortedSet);
        grantParam.setPrivilegeLevel(PrivilegeLevel.DATABASE);
        return grantParam;
    }

    private List<PrivilegePoint> generatePrivilegePoints() {
        List<PrivilegePoint> result = Lists.newArrayList();
        if (createPriv) {
            result.add(PrivilegePoint.CREATE);
        }

        if (alterPriv) {
            result.add(PrivilegePoint.ALTER);
        }

        if (dropPriv) {
            result.add(PrivilegePoint.DROP);
        }

        if (indexPriv) {
            result.add(PrivilegePoint.INDEX);
        }

        if (selectPriv) {
            result.add(PrivilegePoint.SELECT);
        }

        if (insertPriv) {
            result.add(PrivilegePoint.INSERT);
        }

        if (updatePriv) {
            result.add(PrivilegePoint.UPDATE);
        }

        if (deletePriv) {
            result.add(PrivilegePoint.DELETE);
        }

        return result;
    }
}
