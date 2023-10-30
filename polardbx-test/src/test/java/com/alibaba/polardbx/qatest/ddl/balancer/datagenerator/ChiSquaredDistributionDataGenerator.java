package com.alibaba.polardbx.qatest.ddl.balancer.datagenerator;

import com.alibaba.polardbx.qatest.ddl.balancer.datagenerator.random.RandomDouble;
import com.alibaba.polardbx.qatest.ddl.balancer.datagenerator.random.RandomInt;
import com.alibaba.polardbx.qatest.ddl.balancer.datagenerator.random.TimeStampGenerator;
import org.apache.commons.lang3.StringUtils;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public class ChiSquaredDistributionDataGenerator implements DataGenerator {

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

            data = randomInt.chiSquaredDistribution(100_0000, 0);

        } else if (StringUtils.containsIgnoreCase(fieldType, "char")) {

            data = String.valueOf(randomInt.chiSquaredDistribution(100_0000, 0));

        } else if (StringUtils.containsIgnoreCase(fieldType, "double")) {

            data = randomDouble.chiSquaredDistribution();

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

    public RandomDouble getRandomDouble() {
        return randomDouble;
    }

    public void setRandomDouble(RandomDouble randomDouble) {
        this.randomDouble = randomDouble;
    }

    public RandomInt getRandomInt() {
        return randomInt;
    }

    public void setRandomInt(RandomInt randomInt) {
        this.randomInt = randomInt;
    }
}
