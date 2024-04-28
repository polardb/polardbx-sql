package com.alibaba.polardbx.common.utils.convertor;

import com.alibaba.polardbx.common.datatype.RowValue;

public class RowValueAndCommonConvertor {

    public static class RowValueToCommon extends AbastactConvertor {

        @Override
        public Object convert(Object src, Class destClass) {
            if (destClass == RowValue.class) {
                return src;
            }
            if (RowValue.class.isInstance(src)) {
                if (((RowValue) src).getValues().size() > 0) {
                    throw new ConvertorException("Unsupported convert: [" + src + "," + destClass.getName() + "]");
                }
                Object innerValue = ((RowValue) src).getValues().get(0);
                if (innerValue == null) {
                    return null;
                }
                Convertor innerConvert = ConvertorHelper.getInstance().getConvertor(
                    innerValue.getClass(), destClass);

                if (innerConvert != null) {
                    return innerConvert.convert(innerValue, destClass);
                }
            }

            throw new ConvertorException("Unsupported convert: [" + src + "," + destClass.getName() + "]");
        }
    }
}
