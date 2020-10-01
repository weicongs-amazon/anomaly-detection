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

import static com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings.COOLDOWN_MINUTES;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.IndexNotFoundException;

import com.amazon.opendistroforelasticsearch.ad.MemoryTracker;
import com.amazon.opendistroforelasticsearch.ad.common.exception.EndRunException;
import com.amazon.opendistroforelasticsearch.ad.ml.CheckpointDao;
import com.amazon.opendistroforelasticsearch.ad.ml.EntityModel;
import com.amazon.opendistroforelasticsearch.ad.ml.ModelManager;
import com.amazon.opendistroforelasticsearch.ad.ml.ModelManager.ModelType;
import com.amazon.opendistroforelasticsearch.ad.ml.ModelState;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.RateLimiter;

public class PriorityCache implements EntityCache {
    private final Logger LOG = LogManager.getLogger(PriorityCache.class);

    // detector id -> CacheBuffer, weight based
    private final Map<String, CacheBuffer> activeEnities;
    private final CheckpointDao checkpointDao;
    private final int dedicatedCacheSize;
    // LRU Cache
    private Cache<String, ModelState<EntityModel>> inActiveEntities;
    private final MemoryTracker memoryTracker;
    private final ModelManager modelManager;
    private final ReentrantLock lock;
    private final int numberOfTrees;
    private final Clock clock;
    private final Duration modelTtl;
    private final int numMinSamples;
    private Map<String, DoorKeeper> doorKeepers;
    private final RateLimiter restoreRateLimiter;
    private Instant lastThrottledRestoreTime;
    private int coolDownMinutes;

    public PriorityCache(
        CheckpointDao checkpointDao,
        int dedicatedCacheSize,
        Duration inactiveEntityTtl,
        int maxInactiveStates,
        MemoryTracker memoryTracker,
        ModelManager modelManager,
        int numberOfTrees,
        Clock clock,
        ClusterService clusterService,
        Duration modelTtl,
        int numMinSamples,
        Settings settings
    ) {
        this.checkpointDao = checkpointDao;
        this.dedicatedCacheSize = dedicatedCacheSize;

        this.activeEnities = new ConcurrentHashMap<>();
        this.memoryTracker = memoryTracker;
        this.modelManager = modelManager;
        this.lock = new ReentrantLock();
        this.numberOfTrees = numberOfTrees;
        this.clock = clock;
        this.modelTtl = modelTtl;
        this.numMinSamples = numMinSamples;
        this.doorKeepers = new ConcurrentHashMap<>();
        // 1 restore from checkpoint per second allowed.
        this.restoreRateLimiter = RateLimiter.create(1);

        this.inActiveEntities = CacheBuilder
            .newBuilder()
            .expireAfterAccess(inactiveEntityTtl.toHours(), TimeUnit.HOURS)
            .maximumSize(maxInactiveStates)
            .concurrencyLevel(1)
            .build();

        this.lastThrottledRestoreTime = Instant.MIN;
        this.coolDownMinutes = (int)(COOLDOWN_MINUTES.get(settings).getMinutes());
    }

    @Override
    public ModelState<EntityModel> get(String modelId, AnomalyDetector detector, double[] datapoint, String entityName) {
        String detectorId = detector.getDetectorId();
        CacheBuffer buffer = computeBufferIfAbsent(detector, detectorId);
        ModelState<EntityModel> modelState = buffer.get(modelId);
        if (modelState == null) {
            DoorKeeper doorKeeper = doorKeepers.computeIfAbsent(detectorId, id -> {
                // reset every 60 intervals
                return new DoorKeeper(
                    AnomalyDetectorSettings.DOOR_KEEPER_MAX_INSERTION,
                    AnomalyDetectorSettings.DOOR_KEEPER_FAULSE_POSITIVE_RATE,
                    detector.getDetectionIntervalDuration().multipliedBy(60),
                    clock
                );
            });

            // first hit, ignore
            if (doorKeeper != null && doorKeeper.mightContain(modelId) == false) {
                doorKeeper.put(modelId);
                return null;
            }

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

            //addSample(stateToPromote, datapoint);

            // There can be race conditions when the memory is close to full. So we can accept more models we want actually
            // host. Extra models will be cleaned up in the next run.
            if (buffer.dedicatedCacheAvailable() || memoryTracker.isHostingAllowed(detectorId, detector, numberOfTrees)) {
                addSample(stateToPromote, datapoint);
                buffer.put(modelId, stateToPromote);
                maybeRestoreOrTrainModel(modelId, entityName, state);
            } else if (buffer.canReplace(priority)) {
                // TODO: record ejected entity to result index to explain "why
                // my entities do not emit results"
                addSample(stateToPromote, datapoint);
                buffer.replace(modelId, stateToPromote);
                maybeRestoreOrTrainModel(modelId, entityName, state);
            } else {
                // only keep weights in inactive cache to keep it small.
                // It can be dangerous to exceed a few dozen kilobytes, especially
                // in small heap machine like t2.
                inActiveEntities.put(modelId, stateToPromote);
            }
        }

        return modelState;
    }

    private void addSample(ModelState<EntityModel> stateToPromote, double[] datapoint) {
        // add samples
        Queue<double[]> samples = stateToPromote.getModel().getSamples();
        samples.add(datapoint);
        // only keep the recent numMinSamples
        if (samples.size() > this.numMinSamples) {
            samples.remove();
        }
    }

    private void maybeRestoreOrTrainModel(String modelId, String entityName, ModelState<EntityModel> state) {
        EntityModel entityModel = state.getModel();
        // rate limit in case of EsRejectedExecutionException from get threadpool whose queue capacity is 1k
        if (entityModel != null
            && (entityModel.getRcf() == null || entityModel.getThreshold() == null)
            && lastThrottledRestoreTime.plus(Duration.ofMinutes(coolDownMinutes)).isBefore(clock.instant())
            && restoreRateLimiter.tryAcquire()) {
            checkpointDao
                .restoreModelCheckpoint(
                    modelId,
                    ActionListener.wrap(
                        checkpoint -> modelManager.processEntityCheckpoint(checkpoint, modelId, entityName, state),
                        exception -> {
                            if (exception instanceof IndexNotFoundException) {
                                modelManager.processEntityCheckpoint(Optional.empty(), modelId, entityName, state);
                            }
                            if (exception instanceof EsRejectedExecutionException || exception instanceof RejectedExecutionException) {
                                LOG.error("too many requests");
                                lastThrottledRestoreTime = Instant.now();
                            } else {
                                LOG.error("Fail to restore models for " + modelId, exception);
                            }
                        }
                    )
                );
        }
    }

    private CacheBuffer computeBufferIfAbsent(AnomalyDetector detector, String detectorId) {
        return activeEnities.computeIfAbsent(detectorId, k -> {
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

    @Override
    public void maintenance() {
        activeEnities.entrySet().stream().forEach(cacheBufferEntry -> {
            cacheBufferEntry.getValue().maintenance();
            ;
        });
        doorKeepers.entrySet().stream().forEach(doorKeeperEntry -> {
            doorKeeperEntry.getValue().maintenance();
            ;
        });
    }

    /**
     * Permanently deletes models hosted in memory and persisted in index.
     *
     * @param detectorId id the of the detector for which models are to be permanently deleted
     */
    @Override
    public void clear(String detectorId) {
        activeEnities.remove(detectorId);
        checkpointDao.deleteModelCheckpointByDetectorId(detectorId);
        doorKeepers.remove(detectorId);
    }

    /**
     * Compute init progress of a detector.  We use the highest priority entity's
     * init progress as the detector's init progress.
     * @param detectorId Detector Id
     * @return init progress for an detector.  Return 0 if we cannot find such
     * detector in the active entity cache.
     */
    @Override
    public float getInitProgress(String detectorId) {
        return Optional.of(activeEnities)
            .map(entities -> entities.get(detectorId))
            .map(buffer -> buffer.getHighestPriorityEntityId())
            .map(entityIdOptional -> entityIdOptional.get())
            .map(entityId -> getInitProgress(detectorId, entityId))
            .orElse(0f);
    }

    /**
     * Compute init progress for an active entity.
     * @param detectorId Detector Id
     * @param entityId Entity Id
     * @return a number between [0,1].  0 means there is zero init progress or
     * we cannot find the entity in the active entity cache.
     */
    @Override
    public float getInitProgress(String detectorId, String entityId) {
        CacheBuffer cacheBuffer = activeEnities.get(detectorId);
        if (cacheBuffer != null) {
            Optional<EntityModel> modelOptional = cacheBuffer.getModel(entityId);
            // TODO: make it work for shingles.  samples.size() is not the real shingle
            long accumulatedShingles = modelOptional.map(model -> model.getRcf())
                .map(rcf -> rcf.getTotalUpdates())
                .orElseGet(() -> modelOptional.map(model -> model.getSamples()).map(samples -> samples.size()).map(Long::valueOf).orElse(0L));
            return Math.min(1.0f, (float)accumulatedShingles / numMinSamples);
        }
        return 0f;
    }

    /**
     * Get the number of active entities of a detector
     * @param detectorId Detector Id
     * @return The number of active entities
     */
    @Override
    public int getActiveEntities(String detectorId) {
        CacheBuffer cacheBuffer = activeEnities.get(detectorId);
        if (cacheBuffer != null) {
            return cacheBuffer.getActiveEntities();
        }
        return 0;
    }

    /**
     * Whether an entity is active or not
     * @param detectorId The Id of the detector that an entity belongs to
     * @param entityId Entity Id
     * @return Whether an entity is active or not
     */
    @Override
    public boolean isActive(String detectorId, String entityId) {
        CacheBuffer cacheBuffer = activeEnities.get(detectorId);
        if (cacheBuffer != null) {
            return cacheBuffer.isActive(entityId);
        }
        return false;
    }
}
