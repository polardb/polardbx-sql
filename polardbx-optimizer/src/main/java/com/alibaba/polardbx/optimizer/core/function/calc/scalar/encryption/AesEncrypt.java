package com.alibaba.polardbx.optimizer.core.function.calc.scalar.encryption;

import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.encrypt.aes.AesUtil;
import com.alibaba.polardbx.common.utils.encrypt.aes.BlockEncryptionMode;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.exception.FunctionException;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;

import java.util.List;

/**
 * AES(Advanced Encryption Standard)加密函数
 * <p>
 * 输入密钥的长度越长, 安全性越高, 性能越低
 * </p>
 *
 * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/encryption-functions.html#function_aes-encrypt">MySQL aes-encrypt</a>
 */
public class AesEncrypt extends AbstractScalarFunction {
    public AesEncrypt(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    public AesEncrypt() {
        super(null, null);
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (FunctionUtils.isNull(args[0]) || FunctionUtils.isNull(args[1])) {
            return null;
        }
        String mode = (String) ec.getExtraServerVariables().get(ConnectionProperties.BLOCK_ENCRYPTION_MODE);
        BlockEncryptionMode encryptionMode =
            (mode == null) ? BlockEncryptionMode.DEFAULT_MODE : BlockEncryptionMode.parseMode(mode);

        byte[] initVector = null;
        if (encryptionMode.isInitVectorRequired()) {
            if (args.length != 3) {
                throw FunctionException.invalidParamCount(getFunctionNames()[0]);
            }
            initVector = FunctionUtils.parseInitVector(args[2], getOperandType(2), getFunctionNames()[0]);
        }
        byte[] plainTextBytes = DataTypeUtil.convert(getOperandType(0), DataTypes.BytesType, args[0]);
        byte[] keyBytes = DataTypeUtil.convert(getOperandType(1), DataTypes.BytesType, args[1]);

        byte[] crypto = null;
        try {
            crypto = AesUtil.encryptToBytes(encryptionMode, plainTextBytes, keyBytes, initVector);
        } catch (Exception e) {
            logger.warn("AES Encryption failed", e);
        }
        return crypto;
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"AES_ENCRYPT"};
    }
}
