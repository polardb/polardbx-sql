package com.alibaba.polardbx.server.encdb.handler;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.common.encdb.EncdbException;
import com.alibaba.polardbx.common.encdb.cipher.CipherSuite;
import com.alibaba.polardbx.common.encdb.cipher.Envelope;
import com.alibaba.polardbx.common.encdb.enums.Constants;
import com.alibaba.polardbx.common.encdb.enums.HashAlgo;
import com.alibaba.polardbx.common.encdb.utils.HashUtil;
import com.alibaba.polardbx.common.encdb.utils.Utils;
import com.alibaba.polardbx.common.privilege.PrivilegeUtil;
import com.alibaba.polardbx.common.utils.encrypt.SecurityUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.sync.EncdbMekProvisionSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.metadb.encdb.EncdbKey;
import com.alibaba.polardbx.gms.metadb.encdb.EncdbKeyManager;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.server.encdb.EncdbMsgProcessor;
import com.alibaba.polardbx.server.encdb.EncdbServer;
import com.alibaba.polardbx.common.encdb.enums.MsgKeyConstants;

import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

/**
 * @author pangzhaoxing
 */
public class EncdbMekProvisionHandler implements EncdbHandler {

    private final Logger logger = LoggerFactory.getLogger(EncdbMekProvisionHandler.class);

    @Override
    public JSONObject handle(JSONObject request, ServerConnection serverConnection) {
        try {
            CipherSuite cipherSuite =
                new CipherSuite(EncdbServer.teeType, request.getString(MsgKeyConstants.CIPHER_SUITE));
            Constants.EncAlgo encAlgo = Constants.EncAlgo.valueOf(request.getString(MsgKeyConstants.ALGORITHM));
            byte[] envBytes = Utils.base64ToBytes(request.getString(MsgKeyConstants.ENVELOPE));
            Envelope env = Envelope.fromBytes(envBytes);
            env.setCiperSuite(cipherSuite);
            JSONObject envJson = JSON.parseObject(
                new String(env.open(EncdbServer.getInstance().getPemPrivateKey()), StandardCharsets.UTF_8));
            String mek = envJson.getString(MsgKeyConstants.MEK);//already base64 encoded
            byte[] mekBytes = Utils.base64ToBytes(mek);

            verifyOrSetMek(mekBytes, serverConnection);
            serverConnection.getEncdbSessionState().setEncAlgo(encAlgo);
        } catch (Exception e) {
            throw new EncdbException(e);
        }

        return EMPTY;
    }

    /**
     * 1. 第一次插入mek时，生成hash值，持久化到metadb，然后cache都内存，最后sync到所有cn节点
     * 2. 高权限账户支持更新mek，生成hash值，持久化到metadb，然后cache在内存，最后sync到所有cn节点
     * 3. 高权限账户每次连接都会sync mek到所有cn节点，这是用于防止sync失败，用户重试
     */
    public void verifyOrSetMek(byte[] mekBytes, ServerConnection serverConnection) throws NoSuchAlgorithmException {

        if (EncdbKeyManager.getInstance().getMekHash() != null) {
            if (!EncdbKeyManager.getInstance().setMek(mekBytes)) {
                if (EncdbMsgProcessor.checkUserPrivileges(serverConnection, false)) {
                    byte[] mekHash = EncdbKeyManager.createMekHash(mekBytes);
                    //高权限账户允许更新mek
                    EncdbKeyManager.getInstance().replaceMekHash(mekHash);
                    EncdbKeyManager.getInstance().setMek(mekBytes);
                    //必须先更新metadb，再做sync
                    SyncManagerHelper.sync(new EncdbMekProvisionSyncAction(mekBytes), SystemDbHelper.DEFAULT_DB_NAME,
                        SyncScope.ALL, true);
                } else {
                    throw new EncdbException("the mek is wrong");
                }
            } else {
                if (EncdbMsgProcessor.checkUserPrivileges(serverConnection, false)) {
                    //高权限账户每次连接都会sync
                    SyncManagerHelper.sync(new EncdbMekProvisionSyncAction(mekBytes), SystemDbHelper.DEFAULT_DB_NAME,
                        SyncScope.ALL, true);
                }
            }
        } else {
            //第一次密钥分发插入mek hash值的时候，必须要高权限账户
            EncdbMsgProcessor.checkUserPrivileges(serverConnection, true);
            byte[] mekHash = EncdbKeyManager.createMekHash(mekBytes);
            //并发分发密钥时，唯一索引保证只会有一个会成功
            EncdbKeyManager.getInstance().insertMekHash(mekHash);
            EncdbKeyManager.getInstance().setMek(mekBytes);
            //必须先更新metadb，再做sync
            SyncManagerHelper.sync(new EncdbMekProvisionSyncAction(mekBytes), SystemDbHelper.DEFAULT_DB_NAME,
                SyncScope.ALL, true);
        }
    }

}
