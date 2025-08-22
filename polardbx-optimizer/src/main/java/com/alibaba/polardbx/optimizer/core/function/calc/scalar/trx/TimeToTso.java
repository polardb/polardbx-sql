package com.alibaba.polardbx.optimizer.core.function.calc.scalar.trx;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;
import org.apache.commons.collections.MapUtils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

public class TimeToTso extends AbstractScalarFunction {
    public TimeToTso(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        DataType type = getReturnType();
        if (FunctionUtils.isNull(args[0])) {
            return null;
        }

        String sessionTimezone = null;
        if (args.length > 1 && !FunctionUtils.isNull(args[1])) {
            sessionTimezone = (String) args[1];
        }

        if (null == sessionTimezone && null != ec
            && MapUtils.isNotEmpty(ec.getServerVariables())
            && null != (sessionTimezone = (String) ec.getServerVariables().get("time_zone"))) {
            if ("SYSTEM".equalsIgnoreCase(sessionTimezone)) {
                sessionTimezone = (String) ec.getServerVariables().get("system_time_zone");
                if ("CST".equalsIgnoreCase(sessionTimezone)) {
                    sessionTimezone = "GMT+08:00";
                }
            }
        }

        if (null != sessionTimezone) {
            final String trimmed = sessionTimezone.trim();
            if (!trimmed.isEmpty() && ('+' == trimmed.charAt(0) || '-' == trimmed.charAt(0))) {
                // Convert '+08:00' to 'GMT+08:00'
                sessionTimezone = "GMT" + trimmed;
            } else if (!sessionTimezone.equals(trimmed)) {
                sessionTimezone = trimmed;
            }
        }

        Date date;
        try {
            date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(args[0].toString());
        } catch (ParseException e) {
            logger.error("Parse tso failed.", e);
            return null;
        }

        if (null != sessionTimezone) {
            // Convert specified timezone to default timezone.
            TimeZone sourceTimeZone = TimeZone.getTimeZone(sessionTimezone);
            TimeZone targetTimeZone = TimeZone.getDefault();
            long sourceOffset = sourceTimeZone.getRawOffset();
            long targetOffset = targetTimeZone.getRawOffset();
            long offset = targetOffset - sourceOffset;
            date.setTime(date.getTime() + offset);
        }

        String tso = String.valueOf(date.getTime() << 22);

        return type.convertFrom(tso);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"TIME_TO_TSO"};
    }

    public int getScale() {
        return 0;

    }

    public int getPrecision() {
        return 0;
    }
}
