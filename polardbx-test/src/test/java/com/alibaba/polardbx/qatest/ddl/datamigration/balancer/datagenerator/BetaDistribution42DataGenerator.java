package com.alibaba.polardbx.qatest.ddl.datamigration.balancer.datagenerator;

import com.alibaba.polardbx.qatest.ddl.datamigration.balancer.datagenerator.random.RandomDouble;
import com.alibaba.polardbx.qatest.ddl.datamigration.balancer.datagenerator.random.RandomInt;

import java.sql.SQLException;

public class BetaDistribution42DataGenerator implements DataGenerator {

    BetaDistribution24DataGenerator betaDistribution;

    public BetaDistribution42DataGenerator() {
        this.betaDistribution = new BetaDistribution24DataGenerator();
        this.betaDistribution.setRandomDouble(
            new RandomDouble(
                0, 1,
                0, 0.5,
                4, 2
            )
        );
        this.betaDistribution.setRandomInt(
            new RandomInt(this.betaDistribution.getRandomDouble())
        );
    }

    @Override
    public Object generateData(String fieldType) throws SQLException {
        return betaDistribution.generateData(fieldType);
    }
}
