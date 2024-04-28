package com.alibaba.polardbx.gms.metadb.encdb;

import com.alibaba.polardbx.common.encdb.cipher.SymCrypto;
import com.alibaba.polardbx.common.encdb.utils.Utils;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.encrypt.SecurityUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.listener.ConfigListener;
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import com.alibaba.polardbx.gms.listener.impl.MetaDbDataIdBuilder;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.util.MetaDbLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import org.bouncycastle.crypto.CryptoException;

import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;

/**
 * MEK只持久化存储其哈希摘要值，内存中缓存MEK
 * 注意保证mek和mekHash的一致性
 *
 * @author pangzhaoxing
 */
public class EncdbKeyManager extends AbstractLifecycle {

    private static final Logger logger = LoggerFactory.getLogger(EncdbKeyManager.class);

    private static final EncdbKeyManager INSTANCE = new EncdbKeyManager();

    private byte[] mek = null;

    private byte[] mekHash = null;

    private long mekId = -1;

    public static EncdbKeyManager getInstance() {
        if (!INSTANCE.isInited()) {
            synchronized (INSTANCE) {
                if (!INSTANCE.isInited()) {
                    INSTANCE.init();
                }
            }
        }
        return INSTANCE;
    }

    @Override
    protected void doInit() {
        reloadEncKeys();
        setupConfigListener();
    }

    private synchronized void reloadEncKeys() {
        try (Connection conn = MetaDbUtil.getConnection()) {
            EncdbKeyAccessor accessor = new EncdbKeyAccessor();
            accessor.setConnection(conn);
            EncdbKey encdbKey = accessor.getMekHash();
            if (encdbKey != null) {
                this.mekHash = Utils.base64ToBytes(encdbKey.getKey());
                this.mek = null;
                this.mekId = encdbKey.getId();
            }
        } catch (SQLException e) {
            MetaDbLogUtil.META_DB_LOG.error(e);
            throw GeneralUtil.nestedException(e);
        }
    }

    public void insertMekHash(byte[] mekHash) {
        try (Connection conn = MetaDbUtil.getConnection()) {
            EncdbKeyAccessor accessor = new EncdbKeyAccessor();
            accessor.setConnection(conn);
            accessor.insertMekHash(Utils.bytesTobase64(mekHash));
        } catch (SQLException e) {
            MetaDbLogUtil.META_DB_LOG.error(e);
            throw GeneralUtil.nestedException(e);
        }
        MetaDbConfigManager.getInstance().notify(MetaDbDataIdBuilder.ENCDB_KEY_DATA_ID, null);
        // wait for all cn to load metadb
        MetaDbConfigManager.getInstance().sync(MetaDbDataIdBuilder.ENCDB_KEY_DATA_ID);
    }

    public void replaceMekHash(byte[] mekHash) {
        try (Connection conn = MetaDbUtil.getConnection()) {
            EncdbKeyAccessor accessor = new EncdbKeyAccessor();
            accessor.setConnection(conn);
            accessor.replaceMekHash(Utils.bytesTobase64(mekHash));
        } catch (SQLException e) {
            MetaDbLogUtil.META_DB_LOG.error(e);
            throw GeneralUtil.nestedException(e);
        }
        MetaDbConfigManager.getInstance().notify(MetaDbDataIdBuilder.ENCDB_KEY_DATA_ID, null);
        // wait for all cn to load metadb
        MetaDbConfigManager.getInstance().sync(MetaDbDataIdBuilder.ENCDB_KEY_DATA_ID);
    }

    private void setupConfigListener() {
        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            EncdbKeyManager.EncdbKeyConfigListener listener = new EncdbKeyConfigListener();
            MetaDbConfigManager.getInstance().register(MetaDbDataIdBuilder.ENCDB_KEY_DATA_ID, conn);
            MetaDbConfigManager.getInstance().bindListener(MetaDbDataIdBuilder.ENCDB_KEY_DATA_ID, listener);
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                "setup encdb key config_listener failed");
        }
    }

    protected static class EncdbKeyConfigListener implements ConfigListener {
        @Override
        public void onHandleConfig(String dataId, long newOpVersion) {
            EncdbKeyManager.getInstance().reloadEncKeys();
        }
    }

    public byte[] getMek() {
        return mek;
    }

    /**
     * mek和mekHash必须保持一致，所以对两者进行操作时，需要加锁保持修改的原子性
     */
    public synchronized boolean setMek(byte[] mek) throws NoSuchAlgorithmException {
        if (mekHash == null) {
            return false;
        } else {
            if (Arrays.equals(mekHash, createMekHash(mek))) {
                this.mek = mek;
                return true;
            } else {
                return false;
            }
        }
    }

    public long getMekId() {
        return mekId;
    }

    public byte[] getMekHash() {
        return mekHash;
    }

    public static byte[] createMekHash(byte[] mekBytes) throws NoSuchAlgorithmException {
        return SecurityUtil.calcMysqlUserPassword(mekBytes);
    }
}
