package com.alibaba.polardbx.qatest.ddl.datamigration.balancer.datagenerator.random;

import org.apache.commons.math3.distribution.BetaDistribution;
import org.apache.commons.math3.distribution.ChiSquaredDistribution;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.distribution.UniformRealDistribution;

public class RandomDouble {

    UniformRealDistribution uniformRealDistribution;
    NormalDistribution normalDistribution;
    BetaDistribution betaDistribution;
    ChiSquaredDistribution chiSquaredDistribution;

    public RandomDouble(int uniformDistributionLow, int uniformDistributionHi,
                        double normalDistributionMean, double normalDistributionSd,
                        double betaDistributionAlpha, double betaDistributionBeta) {

        uniformRealDistribution = new UniformRealDistribution(uniformDistributionLow, uniformDistributionHi);
        normalDistribution = new NormalDistribution(normalDistributionMean, normalDistributionSd);
        betaDistribution = new BetaDistribution(betaDistributionAlpha, betaDistributionBeta);
        chiSquaredDistribution = new ChiSquaredDistribution(6);
    }

    public double uniformDistribution() {
        return uniformDistribution(1)[0];
    }

    public double[] uniformDistribution(int count) {
        return uniformRealDistribution.sample(count);
    }

    public double normalDistribution() {
        return normalDistribution(1)[0];
    }

    public double[] normalDistribution(int count) {
        return normalDistribution.sample(count);
    }

    public double betaDistribution() {
        return betaDistribution(1)[0];
    }

    public double[] betaDistribution(int count) {
        return betaDistribution.sample(count);
    }

    public double chiSquaredDistribution() {
        return chiSquaredDistribution(1)[0];
    }

    public double[] chiSquaredDistribution(int count) {
        return chiSquaredDistribution.sample(count);
    }

}
