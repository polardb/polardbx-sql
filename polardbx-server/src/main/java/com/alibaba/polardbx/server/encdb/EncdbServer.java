package com.alibaba.polardbx.server.encdb;

import com.alibaba.polardbx.common.encdb.EncdbException;
import com.alibaba.polardbx.common.encdb.enums.AsymmAlgo;
import com.alibaba.polardbx.common.encdb.enums.CCFlags;
import com.alibaba.polardbx.common.encdb.cipher.CipherSuite;
import com.alibaba.polardbx.common.encdb.enums.HashAlgo;
import com.alibaba.polardbx.common.encdb.enums.Symmetric;
import com.alibaba.polardbx.common.encdb.cipher.AsymCrypto;
import com.alibaba.polardbx.common.encdb.enums.Constants;
import com.alibaba.polardbx.common.encdb.utils.HKDF;
import com.alibaba.polardbx.common.encdb.utils.HashUtil;
import com.alibaba.polardbx.common.encdb.enums.TeeType;
import com.alibaba.polardbx.common.encdb.utils.Utils;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.encdb.EncdbKeyManager;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;

import java.io.IOException;
import java.io.StringWriter;
import java.security.InvalidParameterException;
import java.security.KeyPair;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;

/**
 * @author pangzhaoxing
 */
public class EncdbServer extends AbstractLifecycle {

    private static final Logger logger = LoggerFactory.getLogger(EncdbServer.class);

    public static final String ENCDB_VERSION = "1.1.13";

    private static final int ASYMM_ENC_ALGO_KEY_LEN = 4096;

    public static final int NONCE_LEN = 8;

    /**
     * server硬件环境，Mysql使用MOCK
     */
    public static final TeeType teeType = TeeType.MOCK;

    public static final CCFlags ccFlags = CCFlags.RND;

    /**
     * 默认加密套件
     */
    public static final CipherSuite cipherSuite = new CipherSuite(teeType);

    private static final EncdbServer INSTANCE = new EncdbServer();

    private PublicKey publicKey;

    private PrivateKey privateKey;

    private String pemPublicKey;

    private String pemPrivateKey;

    private String publicKeyHash;

    /**
     * 兼容协议，暂时无用
     */
    private String enclaveId = "0";

    public static EncdbServer getInstance() {
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
        initAsymEncAlgo();
    }

    private void initAsymEncAlgo() {
        try {
            KeyPair keyPair = AsymCrypto.generateAsymKeyPair(this.cipherSuite.getAsymmAlgo(), ASYMM_ENC_ALGO_KEY_LEN);
            this.publicKey = keyPair.getPublic();
            this.privateKey = keyPair.getPrivate();

            StringWriter stringPublicKeyWriter = new StringWriter();
            StringWriter stringPrivateKeyWriter = new StringWriter();
            try (JcaPEMWriter pemPublicKeyWriter = new JcaPEMWriter(stringPublicKeyWriter);
                JcaPEMWriter pemPrivateKeyWriter = new JcaPEMWriter(stringPrivateKeyWriter)) {
                pemPublicKeyWriter.writeObject(publicKey);
                pemPrivateKeyWriter.writeObject(privateKey);
            }
            this.pemPublicKey = stringPublicKeyWriter.toString();
            this.pemPrivateKey = stringPrivateKeyWriter.toString();
            this.publicKeyHash = Utils.bytesTobase64(HashUtil.hash(cipherSuite.getHashAlgo(), publicKey.getEncoded()));
        } catch (IOException e) {
            throw new EncdbException(e);
        }
    }

    public static byte[] createDEK(HashAlgo hashAlgo, byte[] mek, byte[] nonce) {
        if (mek == null) {
            throw new EncdbException("the mek can not be null");
        }
        byte[] dek;
        if (hashAlgo == HashAlgo.SM3) {
            dek = HKDF.deriveWithSM3(Symmetric.Params.SM4_128_KEY_SIZE.getVal(), mek, nonce, null);
        } else if (hashAlgo == HashAlgo.SHA256) {
            dek = HKDF.deriveWithSHA256(Symmetric.Params.AES_128_KEY_SIZE.getVal(), mek, nonce, null);
        } else {
            throw new InvalidParameterException("Invalid Hash Alg");
        }
        return dek;
    }

    public static byte[] createNonce() {
        byte[] nonce = new byte[NONCE_LEN];
        new SecureRandom().nextBytes(nonce);
        return nonce;
    }

    public CipherSuite getCipherSuite() {
        return cipherSuite;
    }

    public PublicKey getPublicKey() {
        return publicKey;
    }

    public HashAlgo getHashAlgo() {
        return this.cipherSuite.getHashAlgo();
    }

    public Constants.EncAlgo getSymmAlgo() {
        return this.cipherSuite.getSymmAlgo();
    }

    public AsymmAlgo getAsymmAlgo() {
        return this.cipherSuite.getAsymmAlgo();
    }

    public String getPemPublicKey() {
        return pemPublicKey;
    }

    public String getPemPrivateKey() {
        return pemPrivateKey;
    }

    public String getPublicKeyHash() {
        return publicKeyHash;
    }

    public String getEnclaveId() {
        return enclaveId;
    }

}
