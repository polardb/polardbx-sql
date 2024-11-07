package com.alibaba.polardbx.common.columnar;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;

import java.util.function.Consumer;

/**
 * @author yaozhili
 */
public class ColumnarOption {
    private final String key;
    private final String defaultValue;
    private final String description;
    private final Consumer<Param> setIndex;
    private final Consumer<Param> setGlobal;
    private final Consumer<Param> validator;

    /**
     * @param key option key
     * @param defaultValue default value of this option
     * @param description description of this option
     * @param setIndex Define what will be done when an index option is set. An exception should be thrown if fails.
     * Null means this option is immutable, and should be declared when index is created, and set index option is not supported.
     * @param setGlobal Define what will be done when a global option is set. An exception should be thrown if fails.
     * Null means set global option is not supported.
     * @param validator Validate the setting value. Throw an exception if fail to validate.
     */
    public ColumnarOption(String key, String defaultValue, String description,
                          Consumer<Param> setIndex,
                          Consumer<Param> setGlobal,
                          Consumer<Param> validator) {
        this.key = key;
        this.defaultValue = defaultValue;
        this.description = description;
        this.setIndex = setIndex;
        this.setGlobal = setGlobal;
        this.validator = validator;
    }

    public void handle(Param param) {
        if (null == setGlobal && null == setIndex) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, param.key.toUpperCase()
                + " is not supported to set dynamically.");
        }
        if (null != validator) {
            validator.accept(param);
        }
        if (null == param.tableId || 0 == param.tableId) {
            if (null == setGlobal) {
                throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, param.key.toUpperCase()
                    + " is not supported to set global, please specify a particular columnar index.");
            }
            setGlobal.accept(param);
        } else {
            if (null == setIndex) {
                throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, param.key.toUpperCase()
                    + " is not supported to set for a particular index, "
                    + "please call columnar_config_set(key, value) to set a global config.");
            }
            setIndex.accept(param);
        }
    }

    public String getDefault() {
        return defaultValue;
    }

    public static class Param {
        public String key;
        public String value;
        public String schemaName;
        public String tableName;
        public String indexName;
        public Long tableId;
        public Object serverConnection;

        public Param shallowCopy() {
            Param newParam = new Param();
            newParam.key = this.key;
            newParam.value = this.value;
            newParam.schemaName = this.schemaName;
            newParam.tableName = this.tableName;
            newParam.indexName = this.indexName;
            newParam.tableId = this.tableId;
            return newParam;
        }
    }
}
