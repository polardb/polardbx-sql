package com.alibaba.polardbx.common.encdb.cipher;

import com.alibaba.polardbx.common.encdb.EncdbException;
import com.alibaba.polardbx.common.encdb.enums.CCFlags;
import com.alibaba.polardbx.common.encdb.enums.Constants;
import com.alibaba.polardbx.common.encdb.utils.HashUtil;
import com.alibaba.polardbx.common.encdb.utils.Utils;
import com.google.common.primitives.Bytes;
import org.bouncycastle.crypto.CryptoException;

import java.io.Serializable;
import java.nio.BufferUnderflowException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.zip.DataFormatException;

import static com.alibaba.polardbx.common.encdb.utils.Utils.swapBytesByPivot;

public class CipherForMySQL {

    public static final byte VERSION = 64;
    private final byte[] data;
    private final int type;
    private final Constants.EncAlgo algo;
    private final int nonce_start_inclu, nonce_end_exclu;
    private final int body_start_inclu, body_end_exclu;

    public static CipherForMySQL buildCipher(byte[] data) {
        return new CipherForMySQL(data);
    }

    public static CipherForMySQL buildCipher(int type, Constants.EncAlgo algo) {
        return new CipherForMySQL(type, algo);
    }

    public static boolean verifyCheckCode(List<Byte> data, byte expected) {
        byte computed = xorArray(data);
        return computed == expected;
    }

    public static boolean verifyCheckCode(byte[] data, int start, int end, byte expected) {
        byte computed = xorArray(data, start, end);
        return computed == expected;
    }

    private CipherForMySQL(int type, Constants.EncAlgo algo) {
        this.type = type;
        this.algo = algo;
        this.data = null;
        nonce_start_inclu = -1;
        nonce_end_exclu = -1;
        body_start_inclu = -1;
        body_end_exclu = -1;
    }

    private CipherForMySQL(byte[] data) {
        if (data.length < 11) {  // shortest possible cipher, with no body at all
            throw new EncdbException("cipher cannot be " + data.length + " bytes");
        }
        this.data = data;
        byte checkCode = data[0];
        int version = data[1];
        if (version == 64) {
            if (data.length < 12) {  // shortest possible cipher for this version, with no body at all
                throw new EncdbException("cipher cannot be " + data.length + " bytes");
            }
            if (!verifyCheckCode(data, 1, data.length - 1, checkCode)) {
                throw new EncdbException("cipher data check code verify failed");
            }
            // version 64
            // |code(1)|version(1)|type(1)|algo(1)|nonce(8)|body(x)|
            this.type = data[2] & 0xff;
            this.algo = Constants.EncAlgo.from(data[3] & 0xff);
            this.nonce_start_inclu = 4;
            this.nonce_end_exclu = 12;
            this.body_start_inclu = 12;
            this.body_end_exclu = data.length;
        } else {
            // temporary cipher format
            // |code(1)|type(1)|version&algo(1)|body(x)|nonce(8)|
            if (!verifyCheckCode(data, 1, data.length - 9, checkCode)) {
                throw new EncdbException("cipher data check code verify failed");
            }
            this.type = data[1] & 0xff;
            this.algo = Constants.EncAlgo.from(data[2] & 0b00001111);
            this.nonce_start_inclu = data.length - 8;
            this.nonce_end_exclu = data.length;
            this.body_start_inclu = 3;
            this.body_end_exclu = data.length - 8;
        }
    }

    public byte[] getNonce() {
        return Arrays.copyOfRange(this.data, nonce_start_inclu, nonce_end_exclu);
    }

    public boolean checkNonce(byte[] nonce) {
        if (nonce == null || nonce.length != nonce_end_exclu - nonce_start_inclu) {
            return false;
        }
        for (int i = 0; i < nonce.length; i++) {
            if (nonce[i] != this.data[nonce_start_inclu + i]) {
                return false;
            }
        }
        return true;
    }

    public byte[] decrypt(byte[] key) throws NoSuchAlgorithmException, CryptoException, DataFormatException {
        byte[] ivSub, dataSub;
        byte[] tmpData = null;

        try {
            switch (this.algo) {
            case AES_128_GCM:
                ivSub =
                    Arrays.copyOfRange(this.data, this.body_start_inclu, this.body_start_inclu + SymCrypto.GCMIVLength);
                // EncDB CipherForMySQL format: TAG || DATA, convert to java format: DATA || TAG
                dataSub = Bytes.toArray(swapBytesByPivot(
                    Arrays.copyOfRange(this.data, this.body_start_inclu + SymCrypto.GCMIVLength, this.body_end_exclu),
                    SymCrypto.GCMTagLength));
                tmpData = SymCrypto.aesGcmDecrypt(key, dataSub, ivSub);
                break;
            case SM4_128_GCM:
                ivSub =
                    Arrays.copyOfRange(this.data, this.body_start_inclu, this.body_start_inclu + SymCrypto.GCMIVLength);
                // EncDB CipherForMySQL format: TAG || DATA, convert to java format: DATA || TAG
                dataSub = Bytes.toArray(swapBytesByPivot(
                    Arrays.copyOfRange(this.data, this.body_start_inclu + SymCrypto.GCMIVLength, this.body_end_exclu),
                    SymCrypto.GCMTagLength));
                tmpData = SymCrypto.sm4GcmDecrypt(key, dataSub, ivSub);
                break;
            case AES_128_CBC:
                ivSub =
                    Arrays.copyOfRange(this.data, this.body_start_inclu, this.body_start_inclu + SymCrypto.CBCIVLength);
                dataSub =
                    Arrays.copyOfRange(this.data, this.body_start_inclu + SymCrypto.CBCIVLength, this.body_end_exclu);

                tmpData = SymCrypto.aesCBCDecrypt(key, dataSub, ivSub);
                break;
            case AES_128_ECB:
                tmpData = SymCrypto.aesECBDecrypt(key,
                    Arrays.copyOfRange(this.data, this.body_start_inclu, this.body_end_exclu));
                break;
            case SM4_128_CBC:
                ivSub =
                    Arrays.copyOfRange(this.data, this.body_start_inclu, this.body_start_inclu + SymCrypto.CBCIVLength);
                dataSub =
                    Arrays.copyOfRange(this.data, this.body_start_inclu + SymCrypto.CBCIVLength, this.body_end_exclu);
                tmpData = SymCrypto.sm4CBCDecrypt(key, dataSub, ivSub);
                break;
            case SM4_128_ECB:
                tmpData = SymCrypto.sm4ECBDecrypt(key,
                    Arrays.copyOfRange(this.data, this.body_start_inclu, this.body_end_exclu));
                break;
            case AES_128_CTR:
                ivSub =
                    Arrays.copyOfRange(this.data, this.body_start_inclu, this.body_start_inclu + SymCrypto.CTRIVLength);
                dataSub =
                    Arrays.copyOfRange(this.data, this.body_start_inclu + SymCrypto.CBCIVLength, this.body_end_exclu);
                tmpData = SymCrypto.aesCTRDecrypt(key, dataSub, ivSub);
                break;
            case SM4_128_CTR:
                ivSub =
                    Arrays.copyOfRange(this.data, this.body_start_inclu, this.body_start_inclu + SymCrypto.CTRIVLength);
                dataSub =
                    Arrays.copyOfRange(this.data, this.body_start_inclu + SymCrypto.CBCIVLength, this.body_end_exclu);
                tmpData = SymCrypto.sm4CTRDecrypt(key, dataSub, ivSub);
                break;
            default:
                break;
            }

            if (tmpData == null) {
                throw new EncdbException("decrypt failed");
            }
        } catch (BufferUnderflowException e) {
            throw new DataFormatException("Wrong encdb cipher bytes");
        }

        if (!verifyCheckCode(tmpData, 0, tmpData.length - 2, tmpData[tmpData.length - 1])) {
            throw new EncdbException("plain data check code verify failed");
        }
        return Arrays.copyOfRange(tmpData, 0, tmpData.length - 1);
    }

    /*CTR throw iv */
    private byte[] generateIv(CCFlags flag, byte[] dataIn) throws NoSuchAlgorithmException {
        int ivLen = 0;
        switch (algo) {
        case AES_128_GCM:
        case SM4_128_GCM:
            ivLen = SymCrypto.GCMIVLength;
            if (flag == CCFlags.DET) {
                byte[] tmp = HashUtil.doSHA256(dataIn);
                List<Byte> ivTmp = Bytes.asList(tmp);
                assert ivLen <= tmp.length;
                return Bytes.toArray(ivTmp.subList(0, ivLen));
            } else {
                return Utils.generateIv(ivLen);
            }
        case AES_128_ECB:
        case SM4_128_ECB:
            break;
        case AES_128_CBC:
        case SM4_128_CBC:
            ivLen = SymCrypto.CBCIVLength;
            if (flag == CCFlags.DET) {
                /*result is 32 bytes iv*/
                byte[] tmp =
                    (algo == Constants.EncAlgo.SM4_128_CBC) ? HashUtil.doSM3(dataIn) :
                        HashUtil.doSHA256(dataIn);

                List<Byte> ivTmp = Bytes.asList(tmp);
                assert ivLen <= tmp.length;
                return Bytes.toArray(ivTmp.subList(0, ivLen));
            } else {
                return Utils.generateIv(ivLen);
            }
        default:
            throw new NoSuchAlgorithmException("Unsupported algorithm " + algo.name());
        }
        return new byte[0];
    }

    public byte[] encrypt(CCFlags flag, byte[] key, byte[] inputPlain, byte[] nonce)
        throws NoSuchAlgorithmException, CryptoException {

        List<Byte> encBytes = new ArrayList<>();

        encBytes.add((byte) 0);//for check code
        encBytes.add(VERSION);
        encBytes.add((byte) type);
        encBytes.add((byte) algo.getVal());
        encBytes.addAll(Bytes.asList(nonce));

        byte checkCode = xorArray(Bytes.asList(inputPlain));

        byte[] inputWCheckCode = new byte[inputPlain.length + 1];
        System.arraycopy(inputPlain, 0, inputWCheckCode, 0, inputPlain.length);
        inputWCheckCode[inputWCheckCode.length - 1] = checkCode;

        //prepare iv
        byte[] iv = generateIv(flag, inputWCheckCode);

        //actual encryption
        byte[] tmpData = null;
        switch (algo) {
        case AES_128_GCM:
            encBytes.addAll(Bytes.asList(iv));
            tmpData = SymCrypto.aesGcmEncrypt(key, inputWCheckCode, iv);
            // Java format: DATA || TAG, convert to EncDB CipherV0 format: TAG || DATA
            encBytes.addAll(swapBytesByPivot(tmpData, tmpData.length - SymCrypto.GCMTagLength));
            break;
        case AES_128_ECB:
            encBytes.addAll(Bytes.asList(SymCrypto.aesECBEncrypt(key, inputWCheckCode)));
            break;
        case AES_128_CBC:
            encBytes.addAll(Bytes.asList(iv));
            encBytes.addAll(Bytes.asList(SymCrypto.aesCBCEncrypt(key, inputWCheckCode, iv)));
            break;
        case SM4_128_ECB:
            encBytes.addAll(Bytes.asList(SymCrypto.sm4ECBEncrypt(key, inputWCheckCode)));
            break;
        case SM4_128_CBC:
            encBytes.addAll(Bytes.asList(iv));
            List<Byte> tmp = Bytes.asList(SymCrypto.sm4CBCEncrypt(key, inputWCheckCode, iv));
            encBytes.addAll(tmp);
            break;
        case SM4_128_GCM:
            encBytes.addAll(Bytes.asList(iv));
            tmpData = SymCrypto.sm4GcmEncrypt(key, inputWCheckCode, iv);
            encBytes.addAll(swapBytesByPivot(tmpData, tmpData.length - SymCrypto.GCMTagLength));
            break;
        default:
            throw new NoSuchAlgorithmException("Unsupported algorithm " + algo.name());
        }

        byte[] res = Bytes.toArray(encBytes);
        res[0] = xorArray(res, 1, res.length - 1);//check code;

        return res;
    }

    public static byte xorArray(List<Byte> data) {
        byte ret = 0;

        for (Byte datum : data) {
            ret ^= datum;
        }
        return ret;
    }

    public static byte xorArray(byte[] data, int start, int end) {
        byte ret = 0;

        for (int i = start; i <= end; i++) {
            ret ^= data[i];
        }
        return ret;
    }

    public int getEncType() {
        return this.type;
    }

}
