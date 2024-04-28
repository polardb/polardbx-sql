package com.alibaba.polardbx.common.datatype;

import java.util.List;

public class RowValue {

    private List<Object> values;

    public RowValue(List<Object> values) {
        this.values = values;
    }

    public List<Object> getValues() {
        return values;
    }

    public void setValues(List<Object> values) {
        this.values = values;
    }

    @Override
    public String toString() {
        return "RowValue{" +
            "values=" + values +
            '}';
    }
}
