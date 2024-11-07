package com.alibaba.polardbx.optimizer.core.function.calc.scalar.datatime;

import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.common.utils.time.core.OriginalTimestamp;
import com.alibaba.polardbx.optimizer.config.table.Field;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.DateTimeType;
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class StrToDateTest {

    List<Triple<String, String, OriginalTimestamp>> list;
    StrToDate strToDate;
    ExecutionContext ec;

    @Before
    public void prepare() throws IOException {
        list = new ArrayList<>(
            Arrays.asList(
                new ImmutableTriple<>(
                    "2020-08-01 00:00:00",
                    "%Y-%m-%d %T",
                    new OriginalTimestamp(new MysqlDateTime(2020, 8, 1, 0, 0, 0, 0))
                ),
                new ImmutableTriple<>(
                    "Monday 7th November 2022 13:45:30",
                    "%W %D %M %Y %T",
                    new OriginalTimestamp(new MysqlDateTime(2022, 11, 7, 13, 45, 30, 0))
                ),
                new ImmutableTriple<>(
                    "01:02:03 PM",
                    "%r",
                    new OriginalTimestamp(new MysqlDateTime(0, 0, 0, 13, 2, 3, 0))
                )
            )
        );
    }

    @Test
    public void test() {
        for (Triple<String, String, OriginalTimestamp> triple : list) {
            strToDate = new StrToDate(Arrays.asList(DataTypes.StringType, DataTypes.StringType), new DateTimeType());
            strToDate.setResultField(new Field(new DateTimeType()));
            ec = new ExecutionContext();
            OriginalTimestamp result =
                (OriginalTimestamp) strToDate.compute(new Object[] {triple.getLeft(), triple.getMiddle()}, ec);
            assertEquals(triple.getRight(), result);
        }
    }

}