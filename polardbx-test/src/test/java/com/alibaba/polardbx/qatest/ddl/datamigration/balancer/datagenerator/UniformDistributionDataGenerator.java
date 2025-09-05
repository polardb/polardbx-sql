package com.alibaba.polardbx.qatest.ddl.datamigration.balancer.datagenerator;

import com.alibaba.polardbx.qatest.ddl.datamigration.balancer.datagenerator.random.RandomDouble;
import com.alibaba.polardbx.qatest.ddl.datamigration.balancer.datagenerator.random.RandomInt;
import com.alibaba.polardbx.qatest.ddl.datamigration.balancer.datagenerator.random.TimeStampGenerator;
import org.apache.commons.lang3.StringUtils;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public class UniformDistributionDataGenerator implements DataGenerator {

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

            data = randomInt.uniformDistribution(100_0000, 0);

        } else if (StringUtils.containsIgnoreCase(fieldType, "char")) {

            data = String.valueOf(randomInt.uniformDistribution(100_0000, 0));

        } else if (StringUtils.containsIgnoreCase(fieldType, "double")) {

            data = randomDouble.uniformDistribution();

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
        } else if (StringUtils.containsIgnoreCase(fieldType, "decimal")) {
            data = randomInt.uniformDistribution(100_0000, 0);
        } else if (StringUtils.containsIgnoreCase(fieldType, "time")) {
            data = Timestamp.valueOf(
                TimeStampGenerator.millisToZonedDateTime(timeStampGenerator.generateTs(1000 * 6000),
                    ZoneId.systemDefault()).toLocalDateTime());
        } else if(StringUtils.containsIgnoreCase(fieldType, "year")) {
            data = randomInt.uniformDistribution(50, 50);
        } else if(StringUtils.containsIgnoreCase(fieldType, "binary")) {
            data = String.valueOf(randomInt.uniformDistribution(100_0000, 0));
        } else{
            throw new RuntimeException("not supported");
        }

        return data;
    }

}
