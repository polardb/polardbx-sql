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

package com.alibaba.polardbx.server.encdb;

import com.alibaba.polardbx.common.encdb.enums.CCFlags;
import com.alibaba.polardbx.common.encdb.enums.HashAlgo;
import com.alibaba.polardbx.common.encdb.enums.Constants;

/**
 * @author pangzhaoxing
 */
public class EncdbSessionState {

    private HashAlgo hashAlgo;

    private Constants.EncAlgo encAlgo;

    private CCFlags ccFlags;

    private byte[] dek;

    private byte[] nonce;

    public EncdbSessionState(HashAlgo hashAlgo, Constants.EncAlgo encAlgo, CCFlags ccFlags, byte[] dek, byte[] nonce) {
        this.hashAlgo = hashAlgo;
        this.encAlgo = encAlgo;
        this.ccFlags = ccFlags;
        this.dek = dek;
        this.nonce = nonce;
    }

    public CCFlags getCcFlags() {
        return ccFlags;
    }

    public void setCcFlags(CCFlags ccFlags) {
        this.ccFlags = ccFlags;
    }

    public HashAlgo getHashAlgo() {
        return hashAlgo;
    }

    public void setHashAlgo(HashAlgo hashAlgo) {
        this.hashAlgo = hashAlgo;
    }

    public Constants.EncAlgo getEncAlgo() {
        return encAlgo;
    }

    public void setEncAlgo(Constants.EncAlgo encAlgo) {
        this.encAlgo = encAlgo;
    }

    public byte[] getDek() {
        return dek;
    }

    public void setDek(byte[] dek) {
        this.dek = dek;
    }

    public byte[] getNonce() {
        return nonce;
    }

    public void setNonce(byte[] nonce) {
        this.nonce = nonce;
    }
}
