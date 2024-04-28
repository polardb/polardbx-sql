package com.alibaba.polardbx.common.encdb.enums;

import com.alibaba.polardbx.common.encdb.cipher.SymCrypto;

import java.util.Arrays;

public class Constants {

    public enum Stateless implements OrdinalEnum {
        /*
         用户在enclave中的根密钥在当前session结束后不会过期直到被覆盖
         */
        NeverExpire(0),

        /*
         用户在enclave中的根密钥在当前session结束后自动过期
         */
        SessionExpire(1);

        private final int val;

        Stateless(int val) {
            this.val = val;
        }

        @Override
        public int getVal() {
            return val;
        }
    }

    //DekMode, DekGenMode
    public enum DekGenMode implements OrdinalEnum {

        /*
        用户新的数据密钥在enclave中生成
        */
        ENCLAVE(0),

        /*
        用户新的数据密钥在本地sdk中生成
        */
        LOCAL(1);

        private final int val;

        DekGenMode(int i) {
            this.val = i;
        }

        @Override
        public int getVal() {
            return val;
        }

        public static DekGenMode from(int i) {
            return Arrays.stream(DekGenMode.values())
                .filter(e -> e.val == i)
                .findAny()
                .orElseThrow(() -> new IllegalArgumentException("invalid value"));

        }
    }

    public enum EncScheme implements OrdinalEnum {
        /*
        数据加密过程中初始向量（e.g., iv)会随机生成
        */
        RND(1),

        /*
        数据加密过程中初始向量（e.g., iv)会按规律生成
        */
        DET(2);

        private final int val;

        EncScheme(int i) {
            this.val = i;
        }

        @Override
        public int getVal() {
            return val;
        }

        public static EncScheme from(int i) {
            return Arrays.stream(EncScheme.values())
                .filter(e -> e.val == i)
                .findAny()
                .orElseThrow(() -> new IllegalArgumentException("invalid value"));
        }
    }

    /**
     * Symmetric algorithms to encrypt or decrypt data
     */
    public enum EncAlgo implements OrdinalEnum {
        AES_128_GCM(0),
        AES_128_ECB(1),
        AES_128_CTR(2),
        AES_128_CBC(3),
        SM4_128_CBC(4),
        SM4_128_ECB(5),
        SM4_128_CTR(6),
        SM4_128_GCM(7),

        CLWW_ORE(8);

        private final int val;

        EncAlgo(int val) {
            this.val = val;
        }

        @Override
        public int getVal() {
            return val;
        }

        public static EncAlgo from(int i) {
            return Arrays.stream(EncAlgo.values())
                .filter(e -> e.val == i)
                .findAny()
                .orElseThrow(() -> new IllegalArgumentException("invalid value"));
        }
    }

    public enum SDKMode {
        /*
        该模式下，用户需设置数据库的连接（dbConnection）和mek等参数。该模式支持
        1. 各种数据类型的加解密
        2. 根密钥的导入，数据密钥的导入导出（明文或者密文态），ACL签发，导入KMS密钥等操作
        */
        Default,

        /*
         该模式下，用户需设置数据库的连接（dbConnection），不设置明文的mek。该模式支持
         1. 不支持数据的加解密
         2. 数据密钥的导入导出（密文态），导入KMS密钥等操作
        * */
        NoMekBypass,

        /*
        该模式下，用户设置明文的mek，目标数据库的库名，用户名，TeeType。该模式支持
        1. 支持各种数据类型的加解密
        2. 数据密钥的导入（明文态），生成密态的ACL
         */
        Offline,

        /*
        用户自己表级别单一的DEK,或者自动生成表级别单一的DEK
         */
        SimpleDek,
        /*
        使用服务器端约定的固定DEK，POC会使用
         */
        ConstantDek,

        /*
        CryptoOnly
         */
        Crypto
    }

    public enum KeyMgmtType {
        /*
         * 适用于RDS PG和PolarDB PG模式下
         */
        PG,
        /*
         * 适用于服务化的密文元数据管理系统（也叫keystore）
         */
        KEYSTORE_SERVER,
        /*
         * 适用于PolarDB MySQL（连接proxy的情况下）
         */
        POLARDB_MYSQL,
        /*
         * 适用于RDS MySQL
         */
        RDS_MYSQL,
    }

    /**
     * flags to enable/disable
     */
    public static boolean VERBOSE = false;
    public static boolean SHOW_RA_LOG = false;
    public static boolean BYPASS_RA = false;

    /*JSON KEYS*/
    public static final String ROOT_KEY_ACL = "ACL";
    public static final String ACL_KEY_ALLOW_ENTRIES = "allow entries";
    public static final String ACL_KEY_MAC = "mac";
    public static final String OWNER_ID = "ownerID";
    public static final String USERNAME = "username";
    public static final String USER_ID = "userID";

    public static final int ENCDB_KEY_SIZE = SymCrypto.AES_128_KEY_SIZE;
    public static final int ENCDB_IV_SIZE = SymCrypto.GCMIVLength;
    public static final int ENCDB_MAC_SIZE = SymCrypto.AES_BLOCK_SIZE;

    public enum CipherVersionFormat {
        VERSION_0(0);  // VERSION_0 = static_cast<uint8_t>(0b00000000)

        private final int value;

        CipherVersionFormat(int value) {
            this.value = value;
        }

        public int getValue() {
            return value;
        }
    }

    public static final int CIPHER_VERSION_0_NONCE_LEN = 8;
}


