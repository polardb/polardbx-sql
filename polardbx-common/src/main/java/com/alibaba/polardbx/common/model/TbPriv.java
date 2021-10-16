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

package com.alibaba.polardbx.common.model;

import com.google.common.base.Objects;
import com.alibaba.polardbx.common.utils.TStringUtil;


public class TbPriv {

    private String  dbName;
    private String  tbName;
    private boolean insertPriv;
    private boolean updatePriv;
    private boolean deletePriv;
    private boolean indexPriv;
    private boolean alterPriv;
    private boolean createPriv;
    private boolean dropPriv;
    private boolean grantPriv;
    private boolean selectPriv;

    public TbPriv(String dbName, String tbName){
        this.dbName = TStringUtil.normalizePriv(dbName);
        this.tbName = TStringUtil.normalizePriv(tbName);
    }

    public void loadPriv(long priv) {

        selectPriv = (priv & 1) == 1;
        grantPriv = (priv & 2) == 2;
        dropPriv = (priv & 4) == 4;
        createPriv = (priv & 8) == 8;
        alterPriv = (priv & 16) == 16;
        indexPriv = (priv & 32) == 32;
        deletePriv = (priv & 64) == 64;
        updatePriv = (priv & 128) == 128;
        insertPriv = (priv & 256) == 256;
    }

    public boolean equals(Object other) {
        if (!(other instanceof TbPriv)) {
            return false;
        }

        return Objects.equal(dbName, ((TbPriv) other).getDbName()) &&
               Objects.equal(tbName, ((TbPriv) other).getTbName());
    }

    public int hashCode() {
        return Objects.hashCode(dbName, tbName);
    }

    public String getDbName() {
        return dbName;
    }

    public String getTbName() {
        return tbName;
    }

    public boolean isInsertPriv() {
        return insertPriv;
    }

    public void setInsertPriv(boolean insertPriv) {
        this.insertPriv = insertPriv;
    }

    public boolean isUpdatePriv() {
        return updatePriv;
    }

    public void setUpdatePriv(boolean updatePriv) {
        this.updatePriv = updatePriv;
    }

    public boolean isDeletePriv() {
        return deletePriv;
    }

    public void setDeletePriv(boolean deletePriv) {
        this.deletePriv = deletePriv;
    }

    public boolean isIndexPriv() {
        return indexPriv;
    }

    public void setIndexPriv(boolean indexPriv) {
        this.indexPriv = indexPriv;
    }

    public boolean isAlterPriv() {
        return alterPriv;
    }

    public void setAlterPriv(boolean alterPriv) {
        this.alterPriv = alterPriv;
    }

    public boolean isCreatePriv() {
        return createPriv;
    }

    public void setCreatePriv(boolean createPriv) {
        this.createPriv = createPriv;
    }

    public boolean isDropPriv() {
        return dropPriv;
    }

    public void setDropPriv(boolean dropPriv) {
        this.dropPriv = dropPriv;
    }

    public boolean isGrantPriv() {
        return grantPriv;
    }

    public void setGrantPriv(boolean grantPriv) {
        this.grantPriv = grantPriv;
    }

    public boolean isSelectPriv() {
        return selectPriv;
    }

    public void setSelectPriv(boolean selectPriv) {
        this.selectPriv = selectPriv;
    }
}
