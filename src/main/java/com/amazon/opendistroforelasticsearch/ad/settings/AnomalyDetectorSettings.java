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

package com.amazon.opendistroforelasticsearch.ad.settings;

import java.time.Duration;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.TimeValue;

/**
 * AD plugin settings.
 */
public final class AnomalyDetectorSettings {

    private AnomalyDetectorSettings() {}

    public static final Setting<Integer> MAX_SINGLE_ENTITY_ANOMALY_DETECTORS = Setting
        .intSetting("opendistro.anomaly_detection.max_anomaly_detectors", 1000, Setting.Property.NodeScope, Setting.Property.Dynamic);

    public static final Setting<Integer> MAX_MULTI_ENTITY_ANOMALY_DETECTORS = Setting
        .intSetting(
            "opendistro.anomaly_detection.max_multi_entity_anomaly_detectors",
            10,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final Setting<Integer> MAX_ANOMALY_FEATURES = Setting
        .intSetting("opendistro.anomaly_detection.max_anomaly_features", 5, Setting.Property.NodeScope, Setting.Property.Dynamic);

    public static final Setting<TimeValue> REQUEST_TIMEOUT = Setting
        .positiveTimeSetting(
            "opendistro.anomaly_detection.request_timeout",
            TimeValue.timeValueSeconds(10),
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final Setting<TimeValue> DETECTION_INTERVAL = Setting
        .positiveTimeSetting(
            "opendistro.anomaly_detection.detection_interval",
            TimeValue.timeValueMinutes(10),
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final Setting<TimeValue> DETECTION_WINDOW_DELAY = Setting
        .timeSetting(
            "opendistro.anomaly_detection.detection_window_delay",
            TimeValue.timeValueMinutes(0),
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final Setting<TimeValue> AD_RESULT_ROLLOVER_PERIOD = Setting
        .positiveTimeSetting(
            "opendistro.anomaly_detection.ad_result_rollover_period",
            TimeValue.timeValueHours(12),
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final Setting<TimeValue> AD_RESULT_HISTORY_ROLLOVER_PERIOD = Setting
        .positiveTimeSetting(
            "opendistro.anomaly_detection.ad_result_history_rollover_period",
            TimeValue.timeValueHours(12),
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final Setting<Long> AD_RESULT_HISTORY_MAX_DOCS = Setting
        .longSetting(
            "opendistro.anomaly_detection.ad_result_history_max_docs",
            // Total documents in primary replica.
            // A single feature result is roughly 150 bytes. Suppose a doc is
            // of 200 bytes, 250 million docs is of 50 GB. We choose 50 GB
            // because we have 1 shard at least. One shard can have at most 50 GB.
            250_000_000L,
            0L,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final Setting<TimeValue> AD_RESULT_HISTORY_RETENTION_PERIOD = Setting
        .positiveTimeSetting(
            "opendistro.anomaly_detection.ad_result_history_retention_period",
            TimeValue.timeValueDays(30),
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final Setting<Integer> MAX_RETRY_FOR_UNRESPONSIVE_NODE = Setting
        .intSetting(
            "opendistro.anomaly_detection.max_retry_for_unresponsive_node",
            5,
            0,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final Setting<TimeValue> COOLDOWN_MINUTES = Setting
        .positiveTimeSetting(
            "opendistro.anomaly_detection.cooldown_minutes",
            TimeValue.timeValueMinutes(5),
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final Setting<TimeValue> BACKOFF_MINUTES = Setting
        .positiveTimeSetting(
            "opendistro.anomaly_detection.backoff_minutes",
            TimeValue.timeValueMinutes(15),
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final Setting<TimeValue> BACKOFF_INITIAL_DELAY = Setting
        .positiveTimeSetting(
            "opendistro.anomaly_detection.backoff_initial_delay",
            TimeValue.timeValueMillis(1000),
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final Setting<Integer> MAX_RETRY_FOR_BACKOFF = Setting
        .intSetting("opendistro.anomaly_detection.max_retry_for_backoff", 3, 0, Setting.Property.NodeScope, Setting.Property.Dynamic);

    public static final Setting<Integer> MAX_RETRY_FOR_END_RUN_EXCEPTION = Setting
        .intSetting(
            "opendistro.anomaly_detection.max_retry_for_end_run_exception",
            6,
            0,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    public static final String ANOMALY_DETECTORS_INDEX_MAPPING_FILE = "mappings/anomaly-detectors.json";
    public static final String ANOMALY_DETECTOR_JOBS_INDEX_MAPPING_FILE = "mappings/anomaly-detector-jobs.json";
    public static final String ANOMALY_RESULTS_INDEX_MAPPING_FILE = "mappings/anomaly-results.json";
    public static final String ANOMALY_DETECTION_STATE_INDEX_MAPPING_FILE = "mappings/anomaly-detection-state.json";
    public static final String CHECKPOINT_INDEX_MAPPING_FILE = "mappings/checkpoint.json";

    public static final Duration HOURLY_MAINTENANCE = Duration.ofHours(1);

    public static final Duration CHECKPOINT_TTL = Duration.ofDays(3);

    // ======================================
    // ML parameters
    // ======================================
    // RCF
    public static final int NUM_SAMPLES_PER_TREE = 256;

    public static final int NUM_TREES = 100;

    public static final int TRAINING_SAMPLE_INTERVAL = 64;

    public static final double TIME_DECAY = 0.0001;

    public static final int NUM_MIN_SAMPLES = 128;

    public static final double DESIRED_MODEL_SIZE_PERCENTAGE = 0.0002;

    public static final Setting<Double> MODEL_MAX_SIZE_PERCENTAGE = Setting
        .doubleSetting(
            "opendistro.anomaly_detection.model_max_size_percent",
            0.1,
            0,
            0.7,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    // Thresholding
    public static final double THRESHOLD_MIN_PVALUE = 0.995;

    public static final double THRESHOLD_MAX_RANK_ERROR = 0.0001;

    public static final double THRESHOLD_MAX_SCORE = 8;

    public static final int THRESHOLD_NUM_LOGNORMAL_QUANTILES = 400;

    public static final int THRESHOLD_DOWNSAMPLES = 5_000;

    public static final long THRESHOLD_MAX_SAMPLES = 50_000;

    public static final int MIN_PREVIEW_SIZE = 400; // ok to lower

    // Feature processing
    public static final int MAX_TRAIN_SAMPLE = 24;

    public static final int MAX_SAMPLE_STRIDE = 64;

    public static final int TRAIN_SAMPLE_TIME_RANGE_IN_HOURS = 24;

    public static final int MIN_TRAIN_SAMPLES = 512;

    public static final int DEFAULT_SHINGLE_SIZE = 8;

    public static final int MAX_IMPUTATION_NEIGHBOR_DISTANCE = 2;

    public static final double MAX_SHINGLE_PROPORTION_MISSING = 0.25;

    public static final double PREVIEW_SAMPLE_RATE = 0.25; // ok to adjust, higher for more data, lower for lower latency

    public static final int MAX_PREVIEW_SAMPLES = 300; // ok to adjust, higher for more data, lower for lower latency

    public static final int MAX_PREVIEW_RESULTS = 1_000; // ok to adjust, higher for more data, lower for lower latency

    // AD JOB
    public static final long DEFAULT_AD_JOB_LOC_DURATION_SECONDS = 60;

    // Thread pool
    public static final int AD_THEAD_POOL_QUEUE_SIZE = 1000;

    // multi-entity caching
    public static final int MAX_ACTIVE_STATES = 1000;

    // the size of the cache for small states like last cold start time for an entity.
    // At most, we have 10 multi-entity detector and each one can be hit by 1000 different entities each
    // minute. Since these states' life time is hour, we keep its size 10 * 1000 = 10000.
    public static final int MAX_SMALL_STATES = 10000;

    // Multi-entity detector model setting:
    // TODO (kaituo): change to 4
    public static final int DEFAULT_MULTI_ENTITY_SHINGLE = 1;

    // how many categorical fields we support
    public static final int CATEGORY_FIELD_LIMIT = 1;

    public static final int MULTI_ENTITY_NUM_TREES = 10;

    // cache related
    public static final int DEDICATED_CACHE_SIZE = 10;

    // We only keep priority (4 bytes float) in inactive cache. 1 million priorities
    // take up 4 MB.
    public static final int MAX_INACTIVE_ENTITIES = 1_000_000;

    // TODO: check how much does 1 million insertion costs in memory
    public static final int DOOR_KEEPER_MAX_INSERTION = 1_000_000;

    public static final double DOOR_KEEPER_FAULSE_POSITIVE_RATE = 0.01;

    // Increase the value will adding pressure to indexing anomaly results and our feature query
    public static final Setting<Integer> MAX_ENTITIES_PER_QUERY = Setting
        .intSetting("opendistro.anomaly_detection.max_entities_per_query", 1000, 1, Setting.Property.NodeScope, Setting.Property.Dynamic);

    // save partial zero-anomaly grade results after indexing pressure reaching the limit
    public static final Setting<Float> INDEX_PRESSURE_SOFT_LIMIT = Setting
        .floatSetting(
            "opendistro.anomaly_detection.index_pressure_soft_limit",
            0.8f,
            0.0f,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
        );

    // max number of primary shards of an AD index
    public static final Setting<Integer> MAX_PRIMARY_SHARDS = Setting
        .intSetting("opendistro.anomaly_detection.max_primary_shards", 10, 0, Setting.Property.NodeScope, Setting.Property.Dynamic);

    // max entity value's length
    public static int MAX_ENTITY_LENGTH = 256;

    // max number of index checkpoint requests in one bulk
    public static int MAX_BULK_CHECKPOINT_SIZE = 1000;

    // number of bulk checkpoints per second
    public static double CHECKPOINT_BULK_PER_SECOND = 0.02;

    // responding to 100 cache misses per second allowed.
    // 100 because the get threadpool (the one we need to get checkpoint) queue szie is 1000
    // and we may have 10 concurrent multi-entity detectors. So each detector can use: 1000 / 10 = 100
    public static int MAX_CACHE_HANDLING_PER_SECOND = 100;
}
