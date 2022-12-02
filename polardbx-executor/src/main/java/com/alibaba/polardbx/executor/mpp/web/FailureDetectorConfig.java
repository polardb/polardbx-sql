/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.executor.mpp.web;

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

import javax.validation.constraints.DecimalMax;
import javax.validation.constraints.DecimalMin;
import com.alibaba.polardbx.executor.mpp.metadata.NotNull;
import java.util.concurrent.TimeUnit;

public class FailureDetectorConfig {
    private boolean enabled = true;
    private double failureRatioThreshold = 0.1;
    // ~6secs of failures, given default setting of heartbeatInterval = 500ms
    private Duration heartbeatInterval = new Duration(500, TimeUnit.MILLISECONDS);
    private Duration warmupInterval = new Duration(5, TimeUnit.SECONDS);
    private Duration expirationGraceInterval = new Duration(10, TimeUnit.MINUTES);
    private Duration failureInterval = new Duration(45, TimeUnit.SECONDS);

    @NotNull
    public Duration getExpirationGraceInterval() {
        return expirationGraceInterval;
    }

    @Config("failure-detector.expiration-grace-interval")
    @ConfigDescription("How long to wait before 'forgetting' a service after it disappears from discovery")
    public FailureDetectorConfig setExpirationGraceInterval(Duration expirationGraceInterval) {
        this.expirationGraceInterval = expirationGraceInterval;
        return this;
    }

    public boolean isEnabled() {
        return enabled;
    }

    @Config("failure-detector.enabled")
    public FailureDetectorConfig setEnabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    @NotNull
    public Duration getWarmupInterval() {
        return warmupInterval;
    }

    @Config("failure-detector.warmup-interval")
    @ConfigDescription("How long to wait after transitioning to success before considering a service alive")
    public FailureDetectorConfig setWarmupInterval(Duration warmupInterval) {
        this.warmupInterval = warmupInterval;
        return this;
    }

    @MinDuration("1ms")
    @NotNull
    public Duration getHeartbeatInterval() {
        return heartbeatInterval;
    }

    @Config("failure-detector.heartbeat-interval")
    public FailureDetectorConfig setHeartbeatInterval(Duration interval) {
        this.heartbeatInterval = interval;
        return this;
    }

    @DecimalMin("0.0")
    @DecimalMax("1.0")
    public double getFailureRatioThreshold() {
        return failureRatioThreshold;
    }

    @Config("failure-detector.threshold")
    public FailureDetectorConfig setFailureRatioThreshold(double threshold) {
        this.failureRatioThreshold = threshold;
        return this;
    }

    @NotNull
    public Duration getFailureInterval() {
        return failureInterval;
    }

    @Config("failure-detector.failure-interval")
    public FailureDetectorConfig setFailureInterval(Duration failureInterval) {
        this.failureInterval = failureInterval;
        return this;
    }
}