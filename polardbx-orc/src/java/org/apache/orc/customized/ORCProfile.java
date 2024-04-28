package org.apache.orc.customized;

/**
 * The ORC profile object is adapt to metrics collection in polardbx-orc module.
 */
public interface ORCProfile {
    /**
     * Get profile name.
     */
    String name();

    /**
     * Update the metrics value.
     */
    void update(long delta);
}
