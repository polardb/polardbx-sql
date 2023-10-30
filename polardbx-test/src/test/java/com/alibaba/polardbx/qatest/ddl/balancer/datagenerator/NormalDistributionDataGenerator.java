package com.alibaba.polardbx.qatest.ddl.balancer.datagenerator;

import com.alibaba.polardbx.qatest.ddl.balancer.datagenerator.random.RandomDouble;
import com.alibaba.polardbx.qatest.ddl.balancer.datagenerator.random.RandomInt;
import com.alibaba.polardbx.qatest.ddl.balancer.datagenerator.random.TimeStampGenerator;
import org.apache.commons.lang3.StringUtils;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public class NormalDistributionDataGenerator implements DataGenerator {

    private RandomDouble randomDouble = new RandomDouble(
        0, 1,
        0, 0.5,
        2, 4
    );
    private RandomInt randomInt = new RandomInt(randomDouble);
    private TimeStampGenerator timeStampGenerator = new TimeStampGenerator(ZonedDateTime.now().minusYears(10));

    @Override
    public Object generateData(String fieldType) throws SQLException {
        Object data = null;
        if (StringUtils.containsIgnoreCase(fieldType, "int")) {

            data = randomInt.normalDistribution(10_0000, 5_0000);

        } else if (StringUtils.containsIgnoreCase(fieldType, "char")) {

            data = String.valueOf(randomInt.normalDistribution(10_0000, 5_0000));

        } else if (StringUtils.containsIgnoreCase(fieldType, "double")) {

            data = randomDouble.normalDistribution();

        } else if (StringUtils.containsIgnoreCase(fieldType, "timestamp")) {

            data = Timestamp.valueOf(
                TimeStampGenerator.millisToZonedDateTime(timeStampGenerator.generateTs(1000 * 6000),
                    ZoneId.systemDefault()).toLocalDateTime()
            );

        } else if (StringUtils.containsIgnoreCase(fieldType, "date")) {

            data = Timestamp.valueOf(
                TimeStampGenerator.millisToZonedDateTime(timeStampGenerator.generateTs(1000 * 6000),
                    ZoneId.systemDefault()).toLocalDateTime()
            );

        } else {
            throw new RuntimeException("not supported");
        }

        return data;
    }

}
