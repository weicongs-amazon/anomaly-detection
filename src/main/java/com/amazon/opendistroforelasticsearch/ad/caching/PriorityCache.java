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

import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.MODEL_MAX_SIZE_PERCENTAGE;

import java.time.Clock;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.monitor.jvm.JvmService;

import com.amazon.opendistroforelasticsearch.ad.MemoryTracker;
import com.amazon.opendistroforelasticsearch.ad.NodeStateManager;
import com.amazon.opendistroforelasticsearch.ad.common.exception.EndRunException;
import com.amazon.opendistroforelasticsearch.ad.ml.CheckpointDao;
import com.amazon.opendistroforelasticsearch.ad.ml.EntityModel;
import com.amazon.opendistroforelasticsearch.ad.ml.ModelManager;
import com.amazon.opendistroforelasticsearch.ad.ml.ModelManager.ModelType;
import com.amazon.opendistroforelasticsearch.ad.ml.ModelState;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

public class PriorityCache implements EntityCache {
    private final Logger LOG = LogManager.getLogger(PriorityCache.class);

    // detector id -> CacheBuffer, weight based
    private final Map<String, CacheBuffer> activeEnities;
    private final CheckpointDao checkpointDao;
    private final int dedicatedCacheSize;
    private final NodeStateManager stateManager;
    // LRU Cache
    private Cache<String, ModelState<EntityModel>> inActiveEntities;
    private final MemoryTracker memoryTracker;
    private final ModelManager modelManager;
    private final ReentrantLock lock;
    private final int numberOfTrees;
    private final Clock clock;
    private final DoorKeeper doorKeeper;
    private long heapLimit;
    private int maxInactiveStates;
    private final Duration modelTtl;
    private final int numMinSamples;

    public PriorityCache(
        CheckpointDao checkpointDao,
        int dedicatedCacheSize,
        NodeStateManager stateManager,
        Duration inactiveEntityTtl,
        float maxInactiveEntityStatesPercent,
        int maxInactiveEntityStateBytes,
        JvmService jvmService,
        double modelMaxSizePercentage,
        MemoryTracker memoryTracker,
        ModelManager modelManager,
        int numberOfTrees,
        Clock clock,
        DoorKeeper doorKeeper,
        ClusterService clusterService,
        Duration modelTtl,
        int numMinSamples
    ) {
        this.checkpointDao = checkpointDao;
        this.dedicatedCacheSize = dedicatedCacheSize;
        this.stateManager = stateManager;

        this.activeEnities = new ConcurrentHashMap<>();
        this.memoryTracker = memoryTracker;
        this.modelManager = modelManager;
        this.lock = new ReentrantLock();
        this.numberOfTrees = numberOfTrees;
        this.clock = clock;
        this.doorKeeper = doorKeeper;
        this.modelTtl = modelTtl;
        this.numMinSamples = numMinSamples;

        calculateInactiveCacheLimit(
            jvmService,
            modelMaxSizePercentage,
            maxInactiveEntityStatesPercent,
            maxInactiveEntityStateBytes,
            inactiveEntityTtl
        );

        clusterService
            .getClusterSettings()
            .addSettingsUpdateConsumer(
                MODEL_MAX_SIZE_PERCENTAGE,
                it -> {
                    calculateInactiveCacheLimit(
                        jvmService,
                        it,
                        maxInactiveEntityStatesPercent,
                        maxInactiveEntityStateBytes,
                        inactiveEntityTtl
                    );
                }
            );
    }

    @Override
    public ModelState<EntityModel> get(String modelId, String detectorId, double[] datapoint, String entityName) {
        // first hit
        if (!doorKeeper.mightContain(modelId)) {
            doorKeeper.put(modelId);
            return null;
        }
        CacheBuffer buffer = computeBufferIfAbsent(detectorId);
        ModelState<EntityModel> modelState = buffer.get(modelId);
        if (modelState == null) {
            Optional<AnomalyDetector> detectorOptional = stateManager.getAnomalyDetectorIfPresent(detectorId);
            // we should have detector config available; otherwise, we won't come to this code path
            if (!detectorOptional.isPresent()) {
                throw new EndRunException(detectorId, "AnomalyDetector is not available.", true);
            }
            AnomalyDetector detector = detectorOptional.get();
            // clean up memory if necessary
            clearUpMemoryIfNecessary();

            ModelState<EntityModel> state = inActiveEntities.getIfPresent(modelId);

            // compute updated priority
            float priority = 0;
            if (state != null) {
                priority = state.getPriority();
            }
            priority = buffer.getUpdatedPriority(priority);

            // update state using new priority or create a new one
            if (state != null) {
                state.setPriority(priority);
            } else {
                EntityModel model = new EntityModel(modelId, new ArrayDeque<>(), null, null);
                state = new ModelState<>(model, modelId, detectorId, ModelType.ENTITY.getName(), clock, priority);
            }

            final ModelState<EntityModel> stateToPromote = state;

            // add samples
            Queue<double[]> samples = stateToPromote.getModel().getSamples();
            samples.add(datapoint);
            // only keep the recent numMinSamples
            if (samples.size() > this.numMinSamples) {
                samples.remove();
            }

            // There can be race conditions when the memory is close to full. So we can accept more models we want actually
            // host. Extra models will be cleaned up in the next run.
            if (buffer.dedicatedCacheAvailable() || memoryTracker.isHostingAllowed(detectorId, detector, numberOfTrees)) {
                buffer.put(modelId, stateToPromote);
                maybeRestoreOrTrainModel(modelId, entityName, state);
            } else if (buffer.canReplace(priority)) {
                // TODO: record ejected entity to result index to explain "why
                // my entities do not emit results"
                buffer.replace(modelId, stateToPromote);
                maybeRestoreOrTrainModel(modelId, entityName, state);
            } else {
                inActiveEntities.put(modelId, stateToPromote);
            }
        }
        return modelState;
    }

    private void maybeRestoreOrTrainModel(String modelId, String entityName, ModelState<EntityModel> state) {
        EntityModel entityModel = state.getModel();
        if (entityModel.getRcf() == null || entityModel.getThreshold() == null) {
            checkpointDao
                .restoreModelCheckpoint(
                    modelId,
                    ActionListener
                        .wrap(checkpoint -> modelManager.processEntityCheckpoint(checkpoint, modelId, entityName, state), exception -> {
                            if (exception instanceof IndexNotFoundException) {
                                modelManager.processEntityCheckpoint(Optional.empty(), modelId, entityName, state);
                            } else {
                                LOG.error("Fail to restore models for " + modelId, exception);
                            }
                        })
                );
        }
    }

    private CacheBuffer computeBufferIfAbsent(String detectorId) {
        return activeEnities.computeIfAbsent(detectorId, k -> {
            Optional<AnomalyDetector> detectorOptional = stateManager.getAnomalyDetectorIfPresent(detectorId);
            // we should have detector config available; otherwise, we won't come to this code path
            if (!detectorOptional.isPresent()) {
                throw new EndRunException(detectorId, "AnomalyDetector is not available.", true);
            }
            AnomalyDetector detector = detectorOptional.get();
            long requiredBytes = dedicatedCacheSize * memoryTracker.estimateModelSize(detector, numberOfTrees);
            if (memoryTracker.isHostingAllowed(detectorId, requiredBytes)) {
                memoryTracker.consumeMemory(requiredBytes, true);
                long intervalSecs = detector.getDetectorIntervalInSeconds();
                return new CacheBuffer(
                    dedicatedCacheSize,
                    intervalSecs,
                    checkpointDao,
                    memoryTracker.estimateModelSize(detector, numberOfTrees),
                    memoryTracker,
                    clock,
                    modelTtl
                );
            }
            // if hosting not allowed, exception will be thrown by isHostingAllowed
            throw new EndRunException(detectorId, "Unexpected bug", true);
        });
    }

    private void clearUpMemoryIfNecessary() {
        try {
            if (!lock.tryLock()) {
                return;
            }
            long memoryToShed = memoryTracker.memoryToShed();
            float minPriority = Float.MAX_VALUE;
            CacheBuffer minPriorityBuffer = null;
            while (memoryToShed > 0) {
                for (Map.Entry<String, CacheBuffer> entry : activeEnities.entrySet()) {
                    CacheBuffer buffer = entry.getValue();
                    float priority = buffer.getMinimumPriority();
                    if (buffer.canRemove() && priority < minPriority) {
                        minPriority = priority;
                        minPriorityBuffer = buffer;
                    }
                }
                if (minPriorityBuffer != null) {
                    minPriorityBuffer.remove();
                    long memoryShed = memoryToShed -= minPriorityBuffer.getMemoryConsumptionPerEntity();
                    memoryTracker.releaseMemory(memoryShed, false);
                    memoryToShed -= memoryShed;
                } else {
                    break;
                }
            }
        } finally {
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }

    private void calculateInactiveCacheLimit(
        JvmService jvmService,
        double modelMaxSizePercentage,
        float maxInactiveEntityStatesPercent,
        int maxInactiveEntityStateBytes,
        Duration inactiveEntityTtl
    ) {
        this.heapLimit = (long) (jvmService.info().getMem().getHeapMax().getBytes() * modelMaxSizePercentage);
        long inactiveCacheSize = (long) (this.heapLimit * maxInactiveEntityStatesPercent);
        this.maxInactiveStates = (int) (inactiveCacheSize / maxInactiveEntityStateBytes);
        // reset inactive cache
        this.inActiveEntities = CacheBuilder
            .newBuilder()
            .expireAfterAccess(inactiveEntityTtl.toHours(), TimeUnit.HOURS)
            .maximumSize(maxInactiveStates)
            .concurrencyLevel(1)
            .build();
        memoryTracker.consumeMemory(inactiveCacheSize, true);
    }

    @Override
    public void maintenance() {
        activeEnities.entrySet().stream().forEach(cacheBufferEntry -> {
            cacheBufferEntry.getValue().maintenance();
            ;
        });
    }

    /**
     * Permanently deletes models hosted in memory and persisted in index.
     *
     * @param detectorId id the of the detector for which models are to be permanently deleted
     */
    public void clear(String detectorId) {
        activeEnities.remove(detectorId);
        checkpointDao.deleteModelCheckpointByDetectorId(detectorId);
    }
}
