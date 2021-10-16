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

package com.alibaba.polardbx.optimizer.core.function.calc.scalar.string;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;
import org.apache.commons.lang.StringUtils;

import java.math.BigDecimal;
import java.text.NumberFormat;
import java.util.List;
import java.util.Locale;

/**
 * <pre>
 * FORMAT(X,D)
 *
 * Formats the number X to a format like '#,###,###.##', rounded to D decimal places, and returns the result as a string. If D is 0, the result has no decimal point or fractional part.
 *
 * mysql> SELECT FORMAT(12332.123456, 4);
 *         -> '12,332.1235'
 * mysql> SELECT FORMAT(12332.1,4);
 *         -> '12,332.1000'
 * mysql> SELECT FORMAT(12332.2,0);
 *         -> '12,332'
 *
 *
 * </pre>
 *
 * @author mengshi.sunmengshi 2014年4月15日 下午5:50:13
 * @since 5.1.0
 */
public class Format extends AbstractScalarFunction {
    public Format(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"FORMAT"};
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        for (Object arg : args) {
            if (FunctionUtils.isNull(arg)) {
                return null;
            }
        }

        BigDecimal X = DataTypes.DecimalType.convertFrom(args[0]).toBigDecimal();

        Integer D = DataTypes.IntegerType.convertFrom(args[1]);

        //默认Locale参数
        Locale locale = Locale.US;
        //说明第三个参数可能是Locale信息
        if (args.length > 2) {
            locale = extractLocale(String.valueOf(args[2]));
        }
        NumberFormat nf = NumberFormat.getInstance(locale);
        nf.setGroupingUsed(true);
        nf.setMaximumFractionDigits(D);
        nf.setMinimumFractionDigits(D);
        return nf.format(X);

    }

    /**
     * 根据传入的language_country获取对应的locale对象
     */
    private Locale extractLocale(String languageCountry) {
        if (StringUtils.isBlank(languageCountry)) {
            return Locale.US;
        }
        String[] localeInfos = StringUtils.split(languageCountry, "_");
        if (localeInfos == null || localeInfos.length != 2) {
            return Locale.US;
        }
        for (Locale locale : Locale.getAvailableLocales()) {
            if (StringUtils.equalsIgnoreCase(locale.getLanguage(), localeInfos[0])
                && StringUtils.equalsIgnoreCase(locale.getCountry(), localeInfos[1])) {
                return locale;
            }
        }
        return Locale.US;
    }
}
