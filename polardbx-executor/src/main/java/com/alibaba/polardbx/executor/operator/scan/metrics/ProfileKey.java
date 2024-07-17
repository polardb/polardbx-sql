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

package com.alibaba.polardbx.executor.operator.scan.metrics;

/**
 * This class provides a set of pre-defined profile keys.
 */
public class ProfileKey {
    private final String name;
    private final String description;
    private final ProfileType profileType;
    private final ProfileUnit profileUnit;

    ProfileKey(String name, String description, ProfileType profileType, ProfileUnit profileUnit) {
        this.name = name;
        this.description = description;
        this.profileType = profileType;
        this.profileUnit = profileUnit;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public ProfileUnit getProfileUnit() {
        return profileUnit;
    }

    public ProfileType getProfileType() {
        return profileType;
    }

    public static Builder builder() {
        return new Builder();
    }

    static class Builder {
        private String name;
        private String description;
        private ProfileType profileType;
        private ProfileUnit profileUnit;

        public ProfileKey build() {
            return new ProfileKey(name, description, profileType, profileUnit);
        }

        public Builder setName(String name) {
            this.name = name;
            return this;
        }

        public Builder setProfileUnit(ProfileUnit profileUnit) {
            this.profileUnit = profileUnit;
            return this;
        }

        public Builder setDescription(String description) {
            this.description = description;
            return this;
        }

        public Builder setProfileType(ProfileType profileType) {
            this.profileType = profileType;
            return this;
        }
    }

}
