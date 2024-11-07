package com.alibaba.polardbx.executor.operator;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.executor.mpp.operator.RangeScanMode;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.CursorMeta;

/**
 * @author yuehan.wcf
 */
public class AdaptiveRangeScanClient extends NormalRangeScanClient {
    private final int originPrefetch;

    private final AdaptivePolicy adaptivePolicy;

    public AdaptiveRangeScanClient(ExecutionContext context, CursorMeta meta,
                                   boolean useTransaction, int prefetchNum) {
        super(context, meta, useTransaction, 1, RangeScanMode.ADAPTIVE);
        this.originPrefetch = prefetchNum;
        this.adaptivePolicy =
            AdaptivePolicy.getPolicy(context.getParamManager().getString(ConnectionParams.RANGE_SCAN_ADAPTIVE_POLICY));
    }

    /**
     * Calculates the next prefetch value.
     * This method determines the amount of data to be prefetched based on the current adaptive policy and the existing split index.
     * No parameters are accepted.
     *
     * @return Returns the calculated next prefetch value. The returned value is an integer representing the amount of data to be prefetched.
     */
    public int calcNextPrefetch() {
        // If using the constant policy, directly return the original prefetch value
        if (adaptivePolicy == AdaptivePolicy.CONSTANT) {
            return originPrefetch;
        }
        // Find the next power of two as a potential prefetch value
        int nextPrefetch = findNextPowerOfTwo(pushdownSplitIndex.get());
        // Ensure the final prefetch value does not exceed the original prefetch value
        return Math.min(nextPrefetch, originPrefetch);
    }

    /**
     * Finds the next power of two that is greater than or equal to the given number.
     * If the input number is less than or equal to 0, returns 1.
     * If the number is already a power of two, returns the number itself.
     * Otherwise, calculates and returns the next power of two.
     *
     * @param number The input number.
     * @return The next power of two that is greater than or equal to the input number.
     */
    private static int findNextPowerOfTwo(int number) {
        // If number is less than or equal to 0, the next power of two is 1
        if (number <= 0) {
            return 1;
        }
        // If the number is 2's power, return the number itself.
        if ((number & (number - 1)) == 0) {
            return number;
        }

        // Otherwise, proceed to find the next power of two.
        number |= number >> 1;
        number |= number >> 2;
        number |= number >> 4;
        number |= number >> 8;
        number |= number >> 16;
        return number + 1;
    }

    enum AdaptivePolicy {
        EXPONENT,
        CONSTANT;

        /**
         * Retrieves the corresponding adaptive policy based on the provided policy string.
         *
         * @param policy The policy name, supporting "CONSTANT" and any other value. If "CONSTANT",
         * returns the constant policy; otherwise, returns the exponential policy.
         * @return The adaptive policy object corresponding to the input.
         */
        public static AdaptivePolicy getPolicy(String policy) {
            // Returns the exponential policy by default when the policy string is empty.
            if (StringUtils.isEmpty(policy)) {
                return EXPONENT;
            }
            // Converts the policy string to uppercase and trims leading/trailing spaces for comparison.
            policy = policy.toUpperCase().trim();
            switch (policy) {
            case "CONSTANT":
                return CONSTANT;
            default:
                return EXPONENT;
            }
        }
    }
}
