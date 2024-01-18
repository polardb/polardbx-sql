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

package com.alibaba.polardbx.gms.ha.impl;

import com.alibaba.polardbx.gms.util.PasswdUtil;

import java.util.Objects;

/**
 * @author chenghui.lch
 */
public class StorageNodeHaInfo {

    protected String addr;
    protected StorageRole role;
    protected boolean isHealthy;
    protected int xPort;
    protected String user;
    protected String encPasswd;
    protected final boolean isVip;

    public StorageNodeHaInfo(String addr,
                             StorageRole role,
                             boolean isHealthy,
                             int xPort,
                             String user,
                             String passwd,
                             boolean isVip) {
        this.addr = addr;
        this.role = role;
        this.isHealthy = isHealthy;
        this.xPort = xPort;
        this.user = user;
        this.encPasswd = PasswdUtil.encrypt(passwd);
        this.isVip = isVip;
    }

    public String getAddr() {
        return addr;
    }

    public void setAddr(String addr) {
        this.addr = addr;
    }

    public StorageRole getRole() {
        return role;
    }

    public void setRole(StorageRole role) {
        this.role = role;
    }

    public boolean isHealthy() {
        return isHealthy;
    }

    public void setHealthy(boolean healthy) {
        isHealthy = healthy;
    }

    public int getXPort() {
        return xPort;
    }

    public void setXPort(int xPort) {
        this.xPort = xPort;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (!(object instanceof StorageNodeHaInfo)) {
            return false;
        }
        StorageNodeHaInfo that = (StorageNodeHaInfo) object;
        return isHealthy == that.isHealthy &&
            addr.equals(that.addr) &&
            role == that.role &&
            xPort == that.xPort &&
            user.equals(user) &&
            encPasswd.equals(that.encPasswd) &&
            isVip == that.isVip;
    }

    @Override
    public int hashCode() {
        return Objects.hash(addr, role, isHealthy, xPort, user, encPasswd, isVip);
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getEncPasswd() {
        return encPasswd;
    }

    public void setEncPasswd(String encPasswd) {
        this.encPasswd = encPasswd;
    }

    public boolean isVip() {
        return isVip;
    }
}
