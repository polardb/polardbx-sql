package com.alibaba.polardbx.common.utils.convertor;

import com.alibaba.polardbx.common.datatype.RowValue;
import org.apache.curator.shaded.com.google.common.collect.Lists;

public class CommonAndRowValueConvertor {

    public static class CommonToRowValue extends AbastactConvertor {

        @Override
        public Object convert(Object src, Class destClass) {
            if (RowValue.class.isInstance(src)) {
                return src;
            } else {
                return new RowValue(Lists.newArrayList(src));
            }
        }
    }
}
