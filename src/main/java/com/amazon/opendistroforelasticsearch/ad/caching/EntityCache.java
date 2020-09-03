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
import com.amazon.opendistroforelasticsearch.ad.ml.EntityModel;
import com.amazon.opendistroforelasticsearch.ad.ml.ModelState;

public interface EntityCache extends MaintenanceState {
    /**
     * Get the ModelState associated with the entity.  May or may not load the
     * ModelState depending on the underlying cache's eviction policy.
     *
     * @param modelId Model Id
     * @param detectorId Detector Id
     * @param datapoint The most recent data point
     * @param entityName The Entity's name
     * @return the ModelState associated with the model or null if no cached item
     * for the entity
     */
    ModelState<EntityModel> get(String modelId, String detectorId, double[] datapoint, String entityName);
}
