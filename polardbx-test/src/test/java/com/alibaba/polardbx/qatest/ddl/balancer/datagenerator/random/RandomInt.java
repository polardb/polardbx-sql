package com.alibaba.polardbx.qatest.ddl.balancer.datagenerator.random;

public class RandomInt {

    private RandomDouble randomDouble;

    public RandomInt(RandomDouble randomDouble) {
        this.randomDouble = randomDouble;
    }

    public int uniformDistribution(int scale, int offset) {
        return uniformDistribution(scale, offset, 1)[0];
    }

    public int[] uniformDistribution(int scale, int offset, int count) {
        return MyUtils.scaleAndOffsetToInt(randomDouble.uniformDistribution(count), scale, offset);
    }

    public int normalDistribution(int scale, int offset) {
        return normalDistribution(scale, offset, 1)[0];
    }

    public int[] normalDistribution(int scale, int offset, int count) {
        return MyUtils.double2int(
            MyUtils.offset(
                MyUtils.scale(
                    randomDouble.normalDistribution(count),
                    scale
                ),
                offset
            )
        );
    }

    public int betaDistribution(int scale, int offset) {
        return betaDistribution(scale, offset, 1)[0];
    }

    public int[] betaDistribution(int scale, int offset, int count) {
        return MyUtils.double2int(
            MyUtils.offset(
                MyUtils.scale(
                    randomDouble.betaDistribution(count),
                    scale
                ),
                offset
            )
        );
    }

    public int chiSquaredDistribution(int scale, int offset) {
        return chiSquaredDistribution(scale, offset, 1)[0];
    }

    public int[] chiSquaredDistribution(int scale, int offset, int count) {
        return MyUtils.double2int(
            MyUtils.offset(
                MyUtils.scale(
                    randomDouble.chiSquaredDistribution(count),
                    scale
                ),
                offset
            )
        );
    }

}
