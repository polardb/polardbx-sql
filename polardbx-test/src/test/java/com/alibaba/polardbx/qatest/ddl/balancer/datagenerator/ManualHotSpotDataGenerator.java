package com.alibaba.polardbx.qatest.ddl.balancer.datagenerator;

import com.alibaba.polardbx.qatest.ddl.balancer.datagenerator.random.RandomDouble;
import com.alibaba.polardbx.qatest.ddl.balancer.datagenerator.random.RandomInt;
import com.alibaba.polardbx.qatest.ddl.balancer.datagenerator.random.TimeStampGenerator;
import org.apache.commons.lang3.StringUtils;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.ZoneId;
import java.time.ZonedDateTime;

public class ManualHotSpotDataGenerator implements DataGenerator {

    private RandomDouble randomDouble = new RandomDouble(
        0, 1,
        0, 0.5,
        2, 4
    );
    private RandomInt randomInt = new RandomInt(randomDouble);
    private TimeStampGenerator timeStampGenerator = new TimeStampGenerator(ZonedDateTime.now().minusYears(10));

    @Override
    public Object generateData(String fieldType) throws SQLException {
        if (StringUtils.startsWithIgnoreCase(fieldType, "int") || StringUtils.startsWithIgnoreCase(fieldType,
            "bigint")) {

            //33%的概率在第一个分区
            if (randomDouble.uniformDistribution() < (1.0 / 3.0)) {
                return 0;
            }
            return randomInt.betaDistribution(100_0000, 0);

        } else if (StringUtils.startsWithIgnoreCase(fieldType, "char")) {

            if (randomDouble.uniformDistribution() < (1.0 / 3.0)) {
                return 0;
            }
            return String.valueOf(randomInt.betaDistribution(100_0000, 0));

        } else if (StringUtils.startsWithIgnoreCase(fieldType, "double")) {

            if (randomDouble.uniformDistribution() < (1.0 / 3.0)) {
                return 0;
            }
            return randomDouble.betaDistribution();

        } else if (StringUtils.startsWithIgnoreCase(fieldType, "timestamp")) {

            return Timestamp.valueOf(
                TimeStampGenerator.millisToZonedDateTime(timeStampGenerator.generateTs(1000 * 6000),
                    ZoneId.systemDefault()).toLocalDateTime()
            );

        } else if (StringUtils.containsIgnoreCase(fieldType, "date")) {

            return Timestamp.valueOf(
                TimeStampGenerator.millisToZonedDateTime(timeStampGenerator.generateTs(1000 * 6000),
                    ZoneId.systemDefault()).toLocalDateTime()
            );

        } else {
            throw new RuntimeException("not supported");
        }

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
