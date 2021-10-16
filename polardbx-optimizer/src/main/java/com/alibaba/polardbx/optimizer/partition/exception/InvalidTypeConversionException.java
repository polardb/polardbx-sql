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

package com.alibaba.polardbx.optimizer.partition.exception;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.field.TypeConversionStatus;
import com.alibaba.polardbx.optimizer.partition.datatype.PredicateBoolean;
import com.alibaba.polardbx.optimizer.partition.pruning.PartFieldAccessType;

/**
 * @author chenghui.lch
 */
public class InvalidTypeConversionException extends TddlRuntimeException {

    protected PartFieldAccessType accessType;
    protected TypeConversionStatus status;
    protected PredicateBoolean predicateBoolean;
    protected DataType srcDataType;
    protected DataType tarDataType;

    public InvalidTypeConversionException(PartFieldAccessType accessType, TypeConversionStatus status,
                                          PredicateBoolean predicateBoolean,
                                          DataType srcDataType, DataType tarDataType) {
        super(ErrorCode.ERR_PARTITION_INVALID_DATA_TYPE_CONVERSION,
            buildErrorMsg(accessType, status, predicateBoolean, srcDataType, tarDataType));
        this.srcDataType = srcDataType;
        this.tarDataType = tarDataType;
        this.accessType = accessType;
        this.status = status;
        this.predicateBoolean = predicateBoolean;
    }

    protected static String buildErrorMsg(PartFieldAccessType accessType, TypeConversionStatus status,
                                          PredicateBoolean predicateBoolean,
                                          DataType srcDataType, DataType tarDataType) {
        StringBuilder sb = new StringBuilder("");
        sb.append("invalid type conversion[{").append(srcDataType.getStringSqlType()).append("}->{")
            .append(tarDataType.getStringSqlType()).append("}, ").append(status.toString()).append("/")
            .append(predicateBoolean.name()).append("] in [")
            .append(accessType.getSqlTypeDesc()).append("]");
        return sb.toString();
    }

    public PartFieldAccessType getAccessType() {
        return accessType;
    }

    public TypeConversionStatus getStatus() {
        return status;
    }

    public DataType getSrcDataType() {
        return srcDataType;
    }

    public DataType getTarDataType() {
        return tarDataType;
    }

    public PredicateBoolean getPredicateBoolean() {
        return predicateBoolean;
    }
}
