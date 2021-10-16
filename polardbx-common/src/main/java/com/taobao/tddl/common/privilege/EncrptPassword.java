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

package com.taobao.tddl.common.privilege;

import java.io.Serializable;

public class EncrptPassword implements Serializable {

    private static final long serialVersionUID = 6757582898782090114L;
    private boolean enc = true;
    private String password = null;
    private EncryptAlgorithm encryptAlgorithm = EncryptAlgorithm.SHA1;

    public EncrptPassword() {
    }

    public EncrptPassword(String password, boolean enc) {
        this.password = password;
        this.enc = enc;
        this.encryptAlgorithm = enc ? EncryptAlgorithm.SHA1 : EncryptAlgorithm.NONE;
    }

    public EncrptPassword(boolean enc, String password, EncryptAlgorithm encryptAlgorithm) {
        this.enc = enc;
        this.password = password;
        this.encryptAlgorithm = encryptAlgorithm;
    }

    public enum EncryptAlgorithm {
        NONE,
        SHA1,
        DUAL_SHA1
    }

    public boolean isEnc() {
        return enc;
    }

    public void setEnc(boolean enc) {
        this.enc = enc;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public EncryptAlgorithm getEncryptAlgorithm() {
        return encryptAlgorithm;
    }

    public void setEncryptAlgorithm(EncryptAlgorithm encryptAlgorithm) {
        this.encryptAlgorithm = encryptAlgorithm;
    }
}
