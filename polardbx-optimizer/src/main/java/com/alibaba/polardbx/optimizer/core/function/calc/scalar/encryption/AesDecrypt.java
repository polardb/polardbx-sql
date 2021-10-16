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

package com.alibaba.polardbx.optimizer.core.function.calc.scalar.encryption;

import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.encrypt.aes.AesUtil;
import com.alibaba.polardbx.common.utils.encrypt.aes.BlockEncryptionMode;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;

import java.util.List;

/**
 * AES解密函数
 * <p> 与加密函数{@link AesEncrypt} 相对应</p>
 *
 * @see <a href="https://dev.mysql.com/doc/refman/8.0/en/encryption-functions.html#function_aes-decrypt">MySQL aes-decrypt</a>
 */
public class AesDecrypt extends AbstractScalarFunction {
    public AesDecrypt(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
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
            initVector = FunctionUtils.parseInitVector(args[2], operandTypes.get(2),
                getFunctionNames()[0]);
        }
        byte[] crypto = DataTypeUtil.convert(operandTypes.get(0), DataTypes.BytesType, args[0]);
        byte[] keyBytes = DataTypeUtil.convert(operandTypes.get(1), DataTypes.BytesType, args[1]);

        byte[] plainText = null;
        try {
            plainText = AesUtil.decryptToBytes(encryptionMode, crypto, keyBytes, initVector);
        } catch (Exception e) {
            logger.warn("AES Decryption failed", e);
            // 解密失败则返回null
            return null;
        }
        return new String(plainText);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"AES_DECRYPT"};
    }
}
