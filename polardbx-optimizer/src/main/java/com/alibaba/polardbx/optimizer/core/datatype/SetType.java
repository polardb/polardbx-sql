package com.alibaba.polardbx.optimizer.core.datatype;

import com.alibaba.polardbx.common.type.MySQLStandardFieldType;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

/**
 * This is a workaround for columnar to support MySQL SET type while keeping compatible with old versions
 */
public class SetType extends CharType {

    private final ImmutableList<String> setValues;

    public SetType(List<String> setValues) {
        this.setValues = ImmutableList.copyOf(setValues);
    }

    public ImmutableList<String> getSetValues() {
        return setValues;
    }

    public List<String> convertFromBinary(long binaryValue) {
        List<String> resultSet = new ArrayList<>();
        int setIndex = 0;
        while (binaryValue != 0) {
            if ((binaryValue & 1L) == 1L) {
                resultSet.add(setValues.get(setIndex));
            }
            setIndex++;
            binaryValue >>= 1;
        }
        return resultSet;
    }

    // To achieve full compatibility with MySQL, there are more methods to be implemented ...
}
