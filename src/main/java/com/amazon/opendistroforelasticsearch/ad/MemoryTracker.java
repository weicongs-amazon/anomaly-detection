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

package com.amazon.opendistroforelasticsearch.ad;

import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.MODEL_MAX_SIZE_PERCENTAGE;

import java.util.concurrent.atomic.AtomicLong;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.monitor.jvm.JvmService;

import com.amazon.opendistroforelasticsearch.ad.common.exception.LimitExceededException;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.randomcutforest.RandomCutForest;

/**
 * Class to track AD memory usage.
 *
 */
public class MemoryTracker {
    // A tree of N samples has 2N nodes, with one bounding box for each node.
    private static final int BOUNDING_BOXES = 2;
    // A bounding box has one vector for min values and one for max.
    private static final int VECTORS_IN_BOUNDING_BOX = 2;

    // memory tracker for total consumption of bytes
    private final AtomicLong currentMemoryBytes;
    // reserved for models. Cannot be deleted at will.
    private final AtomicLong reservedMemoryBytes;
    private long heapSize;
    private long heapLimit;
    private long desiredModelSize;
    // constant used to compute an rcf model size
    private long rcfSizeConstant;

    /**
     * Constructor
     *
     * @param jvmService Service providing jvm info
     * @param modelMaxSizePercentage Percentage of heap for the max size of a model
     * @param modelDesiredSizePercentage percentage of heap for the desired size of a model
     * @param clusterService Cluster service object
     * @param sampleSize The sample size used by stream samplers in a RCF forest
     */
    public MemoryTracker(
        JvmService jvmService,
        double modelMaxSizePercentage,
        double modelDesiredSizePercentage,
        ClusterService clusterService,
        int sampleSize
    ) {
        this.currentMemoryBytes = new AtomicLong();
        this.reservedMemoryBytes = new AtomicLong();
        this.heapSize = jvmService.info().getMem().getHeapMax().getBytes();
        this.heapLimit = (long) (heapSize * modelMaxSizePercentage);
        this.desiredModelSize = (long) (heapSize * modelDesiredSizePercentage);
        clusterService
            .getClusterSettings()
            .addSettingsUpdateConsumer(MODEL_MAX_SIZE_PERCENTAGE, it -> this.heapLimit = (long) (heapSize * it));
        this.rcfSizeConstant = sampleSize * BOUNDING_BOXES * VECTORS_IN_BOUNDING_BOX * (Long.SIZE / Byte.SIZE);
    }

    public boolean isHostingAllowed(String detectorId, RandomCutForest rcf) {
        return isHostingAllowed(detectorId, estimateModelSize(rcf));
    }

    /**
     * @param detectorId Detector Id, used in error message
     * @param requiredBytes required bytes in memory
     * @return whether there is memory required for AD
     */
    public boolean isHostingAllowed(String detectorId, long requiredBytes) {
        if (reservedMemoryBytes.get() + requiredBytes <= heapLimit) {
            return true;
        } else {
            throw new LimitExceededException(
                detectorId,
                String
                    .format(
                        "Exceeded memory limit. New size is %d bytes and max limit is %d bytes",
                        reservedMemoryBytes.get() + requiredBytes,
                        heapLimit
                    )
            );
        }
    }

    public boolean isHostingAllowed(String detectorId, AnomalyDetector detector, int numberOfTrees) {
        return isHostingAllowed(detectorId, estimateModelSize(detector, numberOfTrees));
    }

    public synchronized void consumeMemory(long memoryToConsume, boolean reserved) {
        currentMemoryBytes.set(currentMemoryBytes.get() + memoryToConsume);
        if (reserved) {
            reservedMemoryBytes.set(reservedMemoryBytes.get() + memoryToConsume);
        }
    }

    public synchronized void releaseMemory(long memoryToShed, boolean reserved) {
        currentMemoryBytes.set(currentMemoryBytes.get() - memoryToShed);
        if (reserved) {
            reservedMemoryBytes.set(reservedMemoryBytes.get() - memoryToShed);
        }
    }

    /**
     * Gets the estimated size of a RCF model.
     *
     * @param forest RCF configuration
     * @return estimated model size in bytes
     */
    public long estimateModelSize(RandomCutForest forest) {
        return (long) forest.getNumberOfTrees() * (long) forest.getSampleSize() * BOUNDING_BOXES * VECTORS_IN_BOUNDING_BOX * forest
            .getDimensions() * (Long.SIZE / Byte.SIZE);
    }

    /**
     * Gets the estimated size of a RCF model that can be created according to
     * the detector configuration.
     *
     * @param detector detector config object
     * @param numberOfTrees the number of trees in a RCF forest
     * @return estimated model size in bytes
     */
    public long estimateModelSize(AnomalyDetector detector, int numberOfTrees) {
        return rcfSizeConstant * numberOfTrees * detector.getEnabledFeatureIds().size() * detector.getShingleSize();
    }

    /**
     * Bytes to remove to keep AD memory usage within the limit
     * @return bytes to remove
     */
    public long memoryToShed() {
        return currentMemoryBytes.get() - heapLimit;
    }

    /**
     *
     * @return Allowed heap usage in bytes by AD models
     */
    public long getHeapLimit() {
        return heapLimit;
    }

    /**
     *
     * @return Desired model partition size in bytes
     */
    public long getDesiredModelSize() {
        return desiredModelSize;
    }
}
