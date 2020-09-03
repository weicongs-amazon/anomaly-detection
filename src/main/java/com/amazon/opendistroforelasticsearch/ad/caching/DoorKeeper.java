/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.opendistroforelasticsearch.ad.caching;

import com.amazon.opendistroforelasticsearch.ad.MaintenanceState;
import com.google.common.base.Charsets;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

/**
 * A bloom filter placed in front of inactive entity cache to
 * filter out unpopular items that are not likely to appear more
 * than once.
 *
 * Reference: https://arxiv.org/abs/1512.00727
 *
 */
public class DoorKeeper implements MaintenanceState {
    // stores entity's model id
    private BloomFilter<String> bloomFilter;
    // the number of expected insertions to the constructed BloomFilter<T>; must be positive
    private final long expectedInsertions;
    // the desired false positive probability (must be positive and less than 1.0)
    private final double fpp;
    private int flood_gate_delta;

    public DoorKeeper(long expectedInsertions, double fpp, int flood_gate_delta) {
        this.expectedInsertions = expectedInsertions;
        this.fpp = fpp;
        this.flood_gate_delta = flood_gate_delta;
        maintenance();
    }

    public boolean mightContain(String modelId) {
        return bloomFilter.mightContain(modelId);
    }

    public boolean put(String modelId) {
        return bloomFilter.put(modelId);
    }

    /**
     * We reset the bloom filter when the total number of distinct elements
     * exceeds expected insertions or is approaching limit.
     */
    @Override
    public void maintenance() {
        if (bloomFilter == null
            || bloomFilter.approximateElementCount() >= expectedInsertions
            || Math.abs(bloomFilter.approximateElementCount() - expectedInsertions) <= flood_gate_delta) {
            bloomFilter = BloomFilter.create(Funnels.stringFunnel(Charsets.US_ASCII), expectedInsertions, fpp);
        }
    }
}
