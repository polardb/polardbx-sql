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
