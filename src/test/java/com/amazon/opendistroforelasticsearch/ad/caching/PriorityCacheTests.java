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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
//import java.util.Arrays;
import java.util.Map.Entry;
import java.util.Optional;

//import static org.mockito.Mockito.times;
//import static org.mockito.Mockito.verify;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;
//import org.mockito.ArgumentCaptor;

import com.amazon.opendistroforelasticsearch.ad.MemoryTracker;
import com.amazon.opendistroforelasticsearch.ad.ml.CheckpointDao;
import com.amazon.opendistroforelasticsearch.ad.ml.EntityModel;
import com.amazon.opendistroforelasticsearch.ad.ml.ModelManager;
//import com.amazon.opendistroforelasticsearch.ad.ml.ModelState;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyDetector;
import com.amazon.opendistroforelasticsearch.ad.settings.AnomalyDetectorSettings;


public class PriorityCacheTests extends ESTestCase {
    private static final Logger LOG = LogManager.getLogger(PriorityCacheTests.class);

    String modelId1, modelId2;
    CacheProvider cacheProvider;
    CheckpointDao checkpoint;
    MemoryTracker memoryTracker;
    ModelManager modelManager;
    Clock clock;
    ClusterService clusterService;
    Settings settings;
    ThreadPool threadPool;
    float initialPriority;
    CacheBuffer cacheBuffer;
    long memoryPerEntity;
    String detectorId;
    AnomalyDetector detector;
    double[] point;
    String entityName;
    int dedicatedCacheSize;
    Duration detectorDuration;

    @SuppressWarnings("unchecked")
    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        modelId1 = "1";
        modelId2 = "2";
        checkpoint = mock(CheckpointDao.class);
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            ActionListener<Optional<Entry<EntityModel, Instant>>> listener = (ActionListener<
                Optional<Entry<EntityModel, Instant>>>) args[1];
            listener.onResponse(Optional.empty());
            return null;
        }).when(checkpoint).restoreModelCheckpoint(anyString(), any(ActionListener.class));

        memoryTracker = mock(MemoryTracker.class);
        when(memoryTracker.memoryToShed()).thenReturn(0L);

        modelManager = mock(ModelManager.class);

        clock = mock(Clock.class);
        when(clock.instant()).thenReturn(Instant.now());

        clusterService = mock(ClusterService.class);
        settings = Settings.EMPTY;
        threadPool = mock(ThreadPool.class);
        dedicatedCacheSize = 1;

        EntityCache cache = new PriorityCache(
            checkpoint,
            dedicatedCacheSize,
            AnomalyDetectorSettings.CHECKPOINT_TTL,
            AnomalyDetectorSettings.MAX_INACTIVE_ENTITIES,
            memoryTracker,
            modelManager,
            AnomalyDetectorSettings.MULTI_ENTITY_NUM_TREES,
            clock,
            clusterService,
            AnomalyDetectorSettings.HOURLY_MAINTENANCE,
            AnomalyDetectorSettings.NUM_MIN_SAMPLES,
            settings,
            threadPool
        );

        cacheProvider = new CacheProvider(cache);

        memoryPerEntity = 81920L;
        when(memoryTracker.estimateModelSize(any(AnomalyDetector.class), anyInt())).thenReturn(memoryPerEntity);
        when(memoryTracker.canAllocateReserved(anyString(), anyLong())).thenReturn(true);

        detector = mock(AnomalyDetector.class);
        detectorId = "123";
        when(detector.getDetectorId()).thenReturn(detectorId);
        detectorDuration = Duration.ofMinutes(5);
        when(detector.getDetectionIntervalDuration()).thenReturn(detectorDuration);
        when(detector.getDetectorIntervalInSeconds()).thenReturn(detectorDuration.getSeconds());

        cacheBuffer = new CacheBuffer(
            1,
            1,
            checkpoint,
            memoryPerEntity,
            memoryTracker,
            clock,
            AnomalyDetectorSettings.HOURLY_MAINTENANCE,
            detectorId
        );

        initialPriority = cacheBuffer.getUpdatedPriority(0);
        point = new double[] {0.1};
        entityName = "1.2.3.4";
    }

//    public void testCacheHit() {
//        // cache miss due to empty cache
//        assertEquals(null, cacheProvider.get(modelId1,detector, point, entityName));
//        // cache miss due to door keeper
//        assertEquals(null, cacheProvider.get(modelId1,detector, point, entityName));
//        LOG.info(cacheProvider);
//        assertEquals(1, cacheProvider.getActiveEntities(detectorId));
//        ModelState<EntityModel> hitState = cacheProvider.get(modelId1,detector, point, entityName);
//        assertEquals(detectorId, hitState.getDetectorId());
//        EntityModel model = hitState.getModel();
//        assertEquals(null, model.getRcf());
//        assertEquals(null, model.getThreshold());
//        assertTrue(Arrays.equals(point, model.getSamples().peek()));
//
//        ArgumentCaptor<Long> memoryConsumed = ArgumentCaptor.forClass(Long.class);
//        ArgumentCaptor<Boolean> reserved = ArgumentCaptor.forClass(Boolean.class);
//        ArgumentCaptor<MemoryTracker.Origin> origin = ArgumentCaptor.forClass(MemoryTracker.Origin.class);
//
//        verify(memoryTracker, times(1)).consumeMemory(memoryConsumed.capture(), reserved.capture(), origin.capture());
//        assertEquals(dedicatedCacheSize*memoryPerEntity, memoryConsumed.getValue().intValue());
//        assertEquals(true, reserved.getValue().booleanValue());
//        assertEquals(MemoryTracker.Origin.MULTI_ENTITY_DETECTOR, origin.getValue());
//    }
//
//    public void testInActiveCache() {
//        // make modelId1 has enough priority
//        for (int i=0; i<10; i++) {
//            cacheProvider.get(modelId1,detector, point, entityName);
//        }
//        assertEquals(1, cacheProvider.getActiveEntities(detectorId));
//        when(memoryTracker.canAllocate(anyLong())).thenReturn(false);
//        for (int i=0; i<2; i++) {
//            cacheProvider.get(modelId2,detector, point, entityName);
//        }
//        assertEquals(1, cacheProvider.getActiveEntities(detectorId));
//    }
}
