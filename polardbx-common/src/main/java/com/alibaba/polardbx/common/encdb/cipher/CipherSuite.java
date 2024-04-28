package com.alibaba.polardbx.common.encdb.cipher;

import com.alibaba.polardbx.common.encdb.enums.Constants;
import com.alibaba.polardbx.common.encdb.enums.TeeType;
import com.alibaba.polardbx.common.encdb.enums.AsymmAlgo;
import com.alibaba.polardbx.common.encdb.enums.HashAlgo;

import static com.alibaba.polardbx.common.encdb.enums.OrdinalEnum.searchEnum;

public class CipherSuite {

    private final Constants.EncAlgo symmAlgo;
    private final HashAlgo hashAlgo;
    private final AsymmAlgo asymmAlgo;

    public TeeType getTeeType() {
        return teeType;
    }

    final TeeType teeType;

    public HashAlgo getHashAlgo() {
        return hashAlgo;
    }

    public CipherSuite(TeeType backendTeeType, String serverInfoString) {
        teeType = backendTeeType;

        /*
         * serverInfoString returned from backend is in this format
         * SM2_WITH_SM4_128_CBC_SM3 or RSA_WITH_AES_128_CBC_SHA256
         */
        String[] elements = serverInfoString.split("_");
        assert elements.length == 6;

        asymmAlgo = searchEnum(AsymmAlgo.class, elements[0]);
        symmAlgo = searchEnum(Constants.EncAlgo.class, elements[2] + "_" + elements[3] + "_" + elements[4]);
        hashAlgo = searchEnum(HashAlgo.class, elements[5]);
    }

    public CipherSuite(TeeType backendTeeType) {
        this(backendTeeType, getDefaultCipherSuiteByTeeType(backendTeeType));
    }

    public static String getDefaultCipherSuiteByTeeType(TeeType backendTeeType) {
        switch (backendTeeType) {
        case FPGA_SMX:
            return "SM2_WITH_SM4_128_CBC_SM3";
        case MOCK:
            return "SM2_WITH_SM4_128_CBC_SM3";
        default:
            return "RSA_WITH_AES_128_CBC_SHA256";
        }
    }

    public Constants.EncAlgo getSymmAlgo() {
        return symmAlgo;
    }

    public AsymmAlgo getAsymmAlgo() {
        return asymmAlgo;
    }

    @Override
    public String toString() {
        return String.join("_", asymmAlgo.toString(), "WITH", symmAlgo.toString(), hashAlgo.toString());
    }
}
