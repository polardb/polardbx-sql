package com.alibaba.polardbx.executor.vectorized;

import com.alibaba.polardbx.common.utils.time.MySQLTimeConverter;
import com.alibaba.polardbx.common.utils.time.calculator.MySQLIntervalType;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.common.utils.time.core.OriginalDate;
import com.alibaba.polardbx.common.utils.time.core.TimeStorage;
import com.alibaba.polardbx.common.utils.time.parser.TimeParserFlags;
import com.alibaba.polardbx.executor.chunk.DateBlock;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.executor.chunk.ReferenceBlock;
import com.alibaba.polardbx.executor.vectorized.metadata.ExpressionSignatures;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

import java.sql.Types;

import static com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind.Const;
import static com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind.Variable;

@ExpressionSignatures(
    names = {"EXTRACT"},
    argumentTypes = {"Varchar", "Date"},
    argumentKinds = {Const, Variable}
)
public class ExtractVectorizedExpression extends AbstractVectorizedExpression {
    private MySQLIntervalType intervalType;
    private boolean isConstOperandNull;

    public ExtractVectorizedExpression(DataType<?> outputDataType,
                                       int outputIndex, VectorizedExpression[] children) {
        super(outputDataType, outputIndex, children);

        Object operand0Value = ((LiteralVectorizedExpression) children[0]).getConvertedValue();
        String intervalStr = DataTypes.StringType.convertFrom(operand0Value);

        if (intervalStr != null) {
            intervalType = MySQLIntervalType.of(intervalStr);
        }

        isConstOperandNull = intervalType == null;
    }

    @Override
    public void eval(EvaluationContext ctx) {
        evalChildren(ctx);
        MutableChunk chunk = ctx.getPreAllocatedChunk();
        int batchSize = chunk.batchSize();
        boolean isSelectionInUse = chunk.isSelectionInUse();
        int[] sel = chunk.selection();

        // output block
        RandomAccessBlock outputVectorSlot = chunk.slotIn(outputIndex, outputDataType);
        long[] output = (outputVectorSlot.cast(LongBlock.class)).longArray();

        // date block
        RandomAccessBlock inputVectorSlot =
            chunk.slotIn(children[1].getOutputIndex(), children[1].getOutputDataType());

        // when the interval unit is null
        boolean[] outputNulls = outputVectorSlot.nulls();
        if (isConstOperandNull) {
            outputVectorSlot.setHasNull(true);
            for (int i = 0; i < batchSize; i++) {
                outputNulls[i] = true;
            }
            return;
        }

        // handle nulls
        VectorizedExpressionUtils.mergeNulls(chunk, outputIndex, children[1].getOutputIndex());

        if (inputVectorSlot instanceof DateBlock) {
            long[] packedLongs = inputVectorSlot.cast(DateBlock.class).getPacked();

            switch (intervalType) {
            case INTERVAL_YEAR: {
                // for year
                if (isSelectionInUse) {
                    for (int i = 0; i < batchSize; i++) {
                        int j = sel[i];
                        long l = packedLongs[j];
                        output[j] = (l >> 46) / 13;
                    }
                } else {
                    for (int i = 0; i < batchSize; i++) {
                        long l = packedLongs[i];
                        output[i] = (l >> 46) / 13;
                    }
                }
            }
            break;
            case INTERVAL_MONTH: {
                // for month
                if (isSelectionInUse) {
                    for (int i = 0; i < batchSize; i++) {
                        int j = sel[i];
                        long l = packedLongs[j];
                        output[j] = (l >> 46) % 13;
                    }
                } else {
                    for (int i = 0; i < batchSize; i++) {
                        long l = packedLongs[i];
                        output[i] = (l >> 46) % 13;
                    }
                }
            }
            break;
            case INTERVAL_DAY: {
                // for day
                final long modulo = 1L << 5;
                if (isSelectionInUse) {
                    for (int i = 0; i < batchSize; i++) {
                        int j = sel[i];
                        long l = packedLongs[j];
                        output[j] = (l >> 41) % modulo;
                    }
                } else {
                    for (int i = 0; i < batchSize; i++) {
                        long l = packedLongs[i];
                        output[i] = (l >> 41) % modulo;
                    }
                }
            }
            break;
            default:
                // for other interval type, use non-vectorized method.
                boolean isDate = MySQLIntervalType.isDate(intervalType);

                // normal processing for datetime value.
                MysqlDateTime scratchValue = new MysqlDateTime();
                for (int i = 0; i < batchSize; i++) {
                    int j = isSelectionInUse ? sel[i] : i;

                    // parse date value
                    long packedLong = packedLongs[j];
                    TimeStorage.readDate(packedLong, scratchValue);

                    // parse interval by sign and mysql datetime value.
                    int sign = isDate ? 1 : (scratchValue.isNeg() ? -1 : 1);
                    long result = doParseInterval(scratchValue, sign);
                    output[j] = result;
                }
            }
        } else if (inputVectorSlot instanceof ReferenceBlock) {
            // for other interval type, use non-vectorized method.
            boolean isDate = MySQLIntervalType.isDate(intervalType);

            // normal processing for datetime value.
            for (int i = 0; i < batchSize; i++) {
                int j = isSelectionInUse ? sel[i] : i;

                // parse date value
                Object timeObj = inputVectorSlot.elementAt(j);
                MysqlDateTime t;
                int sign;
                if (isDate) {
                    t = DataTypeUtil.toMySQLDatetimeByFlags(timeObj, Types.TIMESTAMP,
                        TimeParserFlags.FLAG_TIME_FUZZY_DATE);
                    if (t == null) {
                        outputNulls[j] = true;
                        continue;
                    }
                    sign = 1;
                } else {
                    t = DataTypeUtil.toMySQLDatetime(timeObj, Types.TIME);
                    if (t == null) {
                        outputNulls[j] = true;
                        continue;
                    }
                    sign = t.isNeg() ? -1 : 1;
                }

                // parse interval by sign and mysql datetime value.
                long result = doParseInterval(t, sign);
                output[j] = result;
            }
        }

    }

    private long doParseInterval(MysqlDateTime t, int sign) {
        switch (intervalType) {
        case INTERVAL_YEAR:
            return t.getYear();
        case INTERVAL_YEAR_MONTH:
            return t.getYear() * 100L + t.getMonth();
        case INTERVAL_QUARTER:
            return (t.getMonth() + 2) / 3;
        case INTERVAL_MONTH:
            return t.getMonth();
        case INTERVAL_WEEK: {
            long[] weekAndYear = MySQLTimeConverter.datetimeToWeek(t, TimeParserFlags.FLAG_WEEK_FIRST_WEEKDAY);
            return weekAndYear[0];
        }
        case INTERVAL_DAY:
            return t.getDay();
        case INTERVAL_DAY_HOUR:
            return (t.getDay() * 100L + t.getHour()) * sign;
        case INTERVAL_DAY_MINUTE:
            return (t.getDay() * 10000L + t.getHour() * 100L + t.getMinute()) * sign;
        case INTERVAL_DAY_SECOND:
            return (t.getDay() * 1000000L + (t.getHour() * 10000L + t.getMinute() * 100 + t.getSecond())) * sign;
        case INTERVAL_HOUR:
            return t.getHour() * sign;
        case INTERVAL_HOUR_MINUTE:
            return (t.getHour() * 100 + t.getMinute()) * sign;
        case INTERVAL_HOUR_SECOND:
            return (t.getHour() * 10000 + t.getMinute() * 100 + t.getSecond()) * sign;
        case INTERVAL_MINUTE:
            return t.getMinute() * sign;
        case INTERVAL_MINUTE_SECOND:
            return (t.getMinute() * 100 + t.getSecond()) * sign;
        case INTERVAL_SECOND:
            return t.getSecond() * sign;
        case INTERVAL_MICROSECOND:
            return t.getSecondPart() / 1000L * sign;
        case INTERVAL_DAY_MICROSECOND:
            return ((t.getDay() * 1000000L + t.getHour() * 10000L + t.getMinute() * 100 + t.getSecond()) * 1000000L + t
                .getSecondPart() / 1000L) * sign;
        case INTERVAL_HOUR_MICROSECOND:
            return ((t.getHour() * 10000L + t.getMinute() * 100 + t.getSecond()) * 1000000L + t.getSecondPart() / 1000L)
                * sign;
        case INTERVAL_MINUTE_MICROSECOND:
            return (((t.getMinute() * 100 + t.getSecond())) * 1000000L + t.getSecondPart() / 1000L) * sign;
        case INTERVAL_SECOND_MICROSECOND:
            return (t.getSecond() * 1000000L + t.getSecondPart() / 1000L) * sign;
        default:
            return 0;
        }
    }
}
