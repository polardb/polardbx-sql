package org.apache.orc.customized;

/**
 * A recyclable object.
 *
 * @param <T> class of entity held by this object.
 */
public interface Recyclable<T> {
    /**
     * Get the entity.
     */
    T get();

    /**
     * Recycle
     */
    void recycle();
}
