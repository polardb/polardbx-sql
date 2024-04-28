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
