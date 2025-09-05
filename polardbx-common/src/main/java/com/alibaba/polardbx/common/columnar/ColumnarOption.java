package com.alibaba.polardbx.common.columnar;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;

import java.util.function.Consumer;


public class ColumnarOption {
    private final String key;
    private final String defaultValue;
    private final String description;
    private final Consumer<Param> setIndex;
    private final Consumer<Param> setGlobal;
    private final Consumer<Param> validator;
    /**
     * ColumnarOption 是列存侧的参数，这里有一个大小写的问题：
     * 1. 列存侧处理这些参数，是从 metadb 的 columnar_config 表读取出来 kv，且很多处理逻辑里是大小写敏感的。
     * 2. 用户的行为肯定是大小写都可能发送过来。
     * 3. metadb 存储的时候，是大小写不敏感的。
     * 4. CN 有一个 map 存储了这些参数，和参数对应的一些校验、处理逻辑，map 是大小写敏感的。
     * 参数的流转过程：用户发送过来 -> CN 的内存 map -> columnar_config 表 -> 列存的内存
     * 综合考虑上述情况，我们的做法是：
     * 1. 用户允许传进来大写或者小写的 key。
     * 2. 用户传进来的 key，统一转大写后，到 map 查询参数的处理逻辑。
     * 2.1 找不到参数，则按用户传进来的 kv 写进 columnar_config（不做大小写转换）。
     * 2.2 找到了参数，则按 caseInsensitive 参数决定怎么写进去 columnar_config。
     * 2.2.1 如果 caseSensitive == LOWERCASE_KEY，则 key 按小写写进去，value 按用户传进来的写进去。
     * 2.2.2 如果 caseSensitive == LOWERCASE_KEY_LOWERCASE_VALUE，则 key 和 value 都按小写写进去。
     * 2.2.3 如果 caseSensitive == UPPERCASE_KEY，则 key 按大写写进去，value 按用户传进来的写进去。
     * 2.2.4 如果 caseSensitive == UPPERCASE_KEY_UPPERCASE_VALUE，则 key 和 value 都按大写写进去。（尽量按照这种处理最好）
     * 2.2.5 如果 caseSensitive == DEFAULT，则 key 和 value 都按用户传进来的写进去。
     */
    private final CaseSensitive caseSensitive;

    public enum CaseSensitive {
        LOWERCASE_KEY,
        LOWERCASE_KEY_LOWERCASE_VALUE,
        UPPERCASE_KEY,
        UPPERCASE_KEY_UPPERCASE_VALUE,
        DEFAULT,
    }

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
                          Consumer<Param> validator,
                          CaseSensitive caseSensitive) {
        this.key = key;
        this.defaultValue = defaultValue;
        this.description = description;
        this.setIndex = setIndex;
        this.setGlobal = setGlobal;
        this.validator = validator;
        this.caseSensitive = caseSensitive;
    }

    public void handle(Param param) {
        if (null == setGlobal && null == setIndex) {
            throw new TddlRuntimeException(ErrorCode.ERR_CONFIG, param.key.toUpperCase()
                + " is not supported to set dynamically.");
        }
        param.caseSensitive = caseSensitive;
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

    public CaseSensitive getCaseSensitive() {
        return caseSensitive;
    }

    public static class Param {
        // Key and value in upper case.
        public String key;
        public String value;
        public CaseSensitive caseSensitive = CaseSensitive.DEFAULT;
        public String schemaName;
        public String tableName;
        public String indexName;
        public Long tableId;
        public Object serverConnection;

        public Param shallowCopy() {
            Param newParam = new Param();
            newParam.key = this.key;
            newParam.value = this.value;
            newParam.caseSensitive = this.caseSensitive;
            newParam.schemaName = this.schemaName;
            newParam.tableName = this.tableName;
            newParam.indexName = this.indexName;
            newParam.tableId = this.tableId;
            return newParam;
        }
    }
}
