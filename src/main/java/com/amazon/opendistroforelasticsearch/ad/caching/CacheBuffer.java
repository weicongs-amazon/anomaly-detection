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

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.cache.RemovalListener;
import org.elasticsearch.common.cache.RemovalNotification;

import com.amazon.opendistroforelasticsearch.ad.MemoryTracker;
import com.amazon.opendistroforelasticsearch.ad.ml.CheckpointDao;
import com.amazon.opendistroforelasticsearch.ad.ml.EntityModel;
import com.amazon.opendistroforelasticsearch.ad.ml.ModelState;

/**
 * We use a layered cache to manage active entities’ states.  We have a two-level
 * cache that stores active entity states in each node.  Each detector has its
 * dedicated cache that stores ten (dynamically adjustable) entities’ states per
 * node.  A detector’s hottest entities load their states in the dedicated cache.
 * If less than 10 entities use the dedicated cache, the secondary cache can use
 * the rest of the free memory available to AD.  The secondary cache is a shared
 * memory among all detectors for the long tail.  The shared cache size is 10%
 * heap minus all of the dedicated cache consumed by single-entity and multi-entity
 * detectors.  The shared cache’s size shrinks as the dedicated cache is filled
 * up or more detectors are started.
 */
public class CacheBuffer {
    private static final Logger LOG = LogManager.getLogger(CacheBuffer.class);

    static class PriorityNode {
        private String key;
        private float priority;

        PriorityNode(String key, float priority) {
            this.priority = priority;
            this.key = key;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            if (obj instanceof PriorityNode) {
                PriorityNode other = (PriorityNode) obj;

                EqualsBuilder equalsBuilder = new EqualsBuilder();
                equalsBuilder.append(key, other.key);

                return equalsBuilder.isEquals();
            }
            return false;
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder().append(key).toHashCode();
        }
    }

    static class PriorityNodeComparator implements Comparator<PriorityNode> {

        @Override
        public int compare(PriorityNode priority, PriorityNode priority2) {
            return Double.compare(priority.priority, priority2.priority);
        }
    }

    private final int minimumCapacity;
    // key -> Priority node
    private final Map<String, PriorityNode> key2Priority;
    private final TreeSet<PriorityNode> priorityMinHeap;
    // key -> value
    private final Map<String, ModelState<EntityModel>> items;
    // when detector is created.  Can be reset.  Unit: seconds
    private long landmarkSecs;
    // length of seconds in one interval.  Used to compute elapsed periods
    // since the detector has been enabled.
    private long intervalSecs;
    // Use ES's RemovalListener instead of Guava's since Guava's RemovalNotification
    // constructor is not public
    private final RemovalListener<String, ModelState<EntityModel>> removalListener;
    // memory consumption per entity
    private final long memoryConsumptionPerEntity;
    private final MemoryTracker memoryTracker;
    private final Clock clock;
    private final CheckpointDao checkpointDao;
    private final Duration modelTtl;

    public CacheBuffer(
        int minimumCapacity,
        long intervalSecs,
        CheckpointDao checkpointDao,
        long memoryConsumptionPerEntity,
        MemoryTracker memoryTracker,
        Clock clock,
        Duration modelTtl
    ) {
        this.minimumCapacity = minimumCapacity;
        this.key2Priority = new HashMap<>();
        this.priorityMinHeap = new TreeSet<>(new PriorityNodeComparator());
        this.items = new HashMap<>();
        this.landmarkSecs = Instant.now().getEpochSecond();
        this.intervalSecs = intervalSecs;
        this.removalListener = new RemovalListener<String, ModelState<EntityModel>>() {
            @Override
            public void onRemoval(RemovalNotification<String, ModelState<EntityModel>> n) {
                checkpointDao.prepareBulk(n.getValue(), n.getKey());
            }
        };
        this.memoryConsumptionPerEntity = memoryConsumptionPerEntity;
        this.memoryTracker = memoryTracker;
        this.clock = clock;
        this.checkpointDao = checkpointDao;
        this.modelTtl = modelTtl;
    }

    /**
     * Update step at period t_k:
     * new priority = old priority + log(1+e^{\log(g(t_k-L))-old priority}) where g(n) = e^{0.5n},
     * and n is the period.
     * @param modelId model Id
     */
    private void update(String modelId) {
        PriorityNode node = key2Priority.computeIfAbsent(modelId, k -> new PriorityNode(modelId, 0f));

        node.priority = getUpdatedPriority(node.priority);

        // reposition this node
        this.priorityMinHeap.remove(node);
        this.priorityMinHeap.add(node);
        items.get(modelId).setLastUsedTime(clock.instant());
    }

    public float getUpdatedPriority(float oldPriority) {
        long increment = computeWeightedCountIncrement();
        // if overflowed, we take the short cut from now on

        oldPriority += Math.log(1 + Math.exp(increment - oldPriority));
        // if overflow happens, using \log(g(t_k-L)) instead.
        if (oldPriority == Float.POSITIVE_INFINITY) {
            oldPriority = increment;
        }
        return oldPriority;
    }

    /**
     * Compute periods relative to landmark and the weighted count increment using 0.125n.
     * Multiply by 0.125 is implemented using right shift for efficiency.
     * @return the weighted count increment used in the priority update step.
     */
    private long computeWeightedCountIncrement() {
        long periods = (Instant.now().getEpochSecond() - landmarkSecs) / intervalSecs;
        return periods >> 3;
    }

    /**
     * Compute the weighted total count by considering landmark
     * \log(C)=\log(\sum_{i=1}^{n} (g(t_i-L)/g(t-L)))=\log(\sum_{i=1}^{n} (g(t_i-L))-\log(g(t-L))
     * @return the minimum priority
     */
    public float getMinimumPriority() {
        PriorityNode smallest = priorityMinHeap.first();
        long periods = (Instant.now().getEpochSecond() - landmarkSecs) / intervalSecs;
        float detectorWeight = periods >> 1;
        return smallest.priority - detectorWeight;
    }

    /**
     * Insert the model state associated with a model Id to the cache
     * @param modelId the model Id
     * @param value the ModelState
     */
    public void put(String modelId, ModelState<EntityModel> value) {
        put(modelId, value, getUpdatedPriority(value.getPriority()));
    }

    /**
    * Insert the model state associated with a model Id to the cache.  Update priority.
    * @param modelId the model Id
    * @param value the ModelState
    * @param priority the priority
    */
    private void put(String modelId, ModelState<EntityModel> value, float priority) {
        if (minimumCapacity <= 0) {
            return;
        }
        ModelState<EntityModel> contentNode = items.get(modelId);
        if (contentNode == null) {
            PriorityNode node = new PriorityNode(modelId, priority);
            key2Priority.put(modelId, node);
            priorityMinHeap.add(node);
            items.put(modelId, value);
            value.setLastUsedTime(clock.instant());
        } else {
            update(modelId);
            items.put(modelId, value);
        }
        if (value != null && value.getModel() != null && value.getModel().getRcf() != null) {
            long memoryToShed = memoryTracker.estimateModelSize(value.getModel().getRcf());
            memoryTracker.consumeMemory(memoryToShed, dedicatedCacheAvailable());
        }
    }

    /**
     * Retrieve the ModelState associated with the model Id or null if the CacheBuffer
     * contains no mapping for the model Id
     * @param key the model Id
     * @return the Model state to which the specified model Id is mapped, or null
     * if this CacheBuffer contains no mapping for the model Id
     */
    public ModelState<EntityModel> get(String key) {
        ModelState<EntityModel> node = items.get(key);
        if (node == null) {
            return null;
        }
        update(key);
        return node;
    }

    /**
     *
     * @return whether there is one item that can be removed from shared cache
     */
    public boolean canRemove() {
        return !items.isEmpty() && items.size() > minimumCapacity;
    }

    /**
     * remove the smallest priority item
     */
    public void remove() {
        PriorityNode smallest = priorityMinHeap.pollFirst();
        if (smallest != null) {
            String keyToRemove = smallest.key;
            ModelState<EntityModel> valueRemoved = remove(keyToRemove);
            removalListener
                .onRemoval(
                    new RemovalNotification<String, ModelState<EntityModel>>(
                        keyToRemove,
                        valueRemoved,
                        RemovalNotification.RemovalReason.REPLACED
                    )
                );
        }
    }

    /**
     * Remove everything associated with the key instead of the PriorityNode in
     * priorityMinHeap.
     *
     * @param keyToRemove The key to remove
     * @return the associated ModelState associated with the key, or null if there
     * is no associated ModelState for the key
     */
    private ModelState<EntityModel> remove(String keyToRemove) {
        key2Priority.remove(keyToRemove);
        ModelState<EntityModel> valueRemoved = items.remove(keyToRemove);
        if (valueRemoved != null && valueRemoved.getModel() != null && valueRemoved.getModel().getRcf() != null) {
            long memoryToShed = memoryTracker.estimateModelSize(valueRemoved.getModel().getRcf());
            memoryTracker.releaseMemory(memoryToShed, dedicatedCacheFull());
        }
        return valueRemoved;
    }

    /**
     * @return whether dedicated cache is available or not
     */
    public boolean dedicatedCacheAvailable() {
        return items.size() < minimumCapacity;
    }

    /**
     * @return whether dedicated cache is full or not
     */
    public boolean dedicatedCacheFull() {
        return items.size() <= minimumCapacity;
    }

    /**
     *
     * @return the estimated number of bytes per entity state
     */
    public long getMemoryConsumptionPerEntity() {
        return memoryConsumptionPerEntity;
    }

    /**
     *
     * If the cache is not full, check if some other items can replace internal entities.
     * @param priority another entity's priority
     * @return whether one entity can be replaced by another entity with a certain priority
     */
    public boolean canReplace(float priority) {
        return !items.isEmpty() && priority > getMinimumPriority();
    }

    /**
     * Replace the smallest priority entity with the input entity
     * @param modelId the Model Id
     * @param value the model State
     */
    public void replace(String modelId, ModelState<EntityModel> value) {
        remove();
        put(modelId, value);
    }

    public void maintenance() {
        List<PriorityNode> toRemove = new ArrayList<>();
        items.entrySet().stream().forEach(entry -> {
            String modelId = entry.getKey();
            try {
                ModelState<EntityModel> modelState = entry.getValue();
                Instant now = clock.instant();

                checkpointDao.prepareBulk(modelState, modelId);

                if (modelState.getLastUsedTime().plus(modelTtl).isBefore(now)) {
                    toRemove.add(new PriorityNode(modelId, modelState.getPriority()));
                }
            } catch (Exception e) {
                LOG.warn("Failed to finish maintenance for model id " + modelId, e);
            }
        });
        // We cannot remove inside the above forEach loop because the code throws
        // ConcurrentModificationException if we do so. This is not a problem
        // if items is a ConcurrentHashMap. We don't use ConcurrentHashMap
        // because it is inherently more complex and costly.
        toRemove.forEach(item -> {
            priorityMinHeap.remove(item);
            remove(item.key);
        });
    }
}
