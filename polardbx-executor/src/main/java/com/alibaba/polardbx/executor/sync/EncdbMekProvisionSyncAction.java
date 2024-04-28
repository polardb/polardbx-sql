package com.alibaba.polardbx.executor.sync;

import com.alibaba.polardbx.common.encdb.EncdbException;
import com.alibaba.polardbx.common.encdb.enums.HashAlgo;
import com.alibaba.polardbx.common.encdb.utils.HashUtil;
import com.alibaba.polardbx.common.utils.encrypt.SecurityUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.gms.metadb.encdb.EncdbKeyManager;

import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

/**
 * @author pangzhaoxing
 */
public class EncdbMekProvisionSyncAction implements ISyncAction {

    private static final Logger logger = LoggerFactory.getLogger(EncdbMekProvisionSyncAction.class);

    private byte[] mek;

    public EncdbMekProvisionSyncAction(byte[] mek) {
        this.mek = mek;
    }

    public byte[] getMek() {
        return mek;
    }

    public void setMek(byte[] mek) {
        this.mek = mek;
    }

    @Override
    public ResultCursor sync() {
        try {
            if (!EncdbKeyManager.getInstance().setMek(mek)) {
                throw new EncdbException("the mekHash is inconsistent with mek");
            }
        } catch (Exception e) {
            throw new EncdbException("sync mek failed", e);
        }
        return null;
    }

}
