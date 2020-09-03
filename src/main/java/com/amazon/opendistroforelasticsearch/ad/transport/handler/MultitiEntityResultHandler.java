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

package com.amazon.opendistroforelasticsearch.ad.transport.handler;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

import com.amazon.opendistroforelasticsearch.ad.indices.AnomalyDetectionIndices;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyResult;
import com.amazon.opendistroforelasticsearch.ad.util.ClientUtil;
import com.amazon.opendistroforelasticsearch.ad.util.IndexUtils;
import com.amazon.opendistroforelasticsearch.ad.util.ThrowingConsumerWrapper;

/**
 * Quick way to create a result handler without remembering all of the low level details.
 * Also, EntityResultTransportAction depends on this class.  All transport actions
 * needs dependency injection.  Guice has a hard time initializing generics class AnomalyIndexHandler &lt; AnomalyResult &gt;
 * due to type erasure. To avoid that, I create a class with a built-in details so
 * that Guice would be able to work out the details.
 *
 */
public class MultitiEntityResultHandler extends AnomalyIndexHandler<AnomalyResult> {

    @Inject
    public MultitiEntityResultHandler(
        Client client,
        Settings settings,
        ThreadPool threadPool,
        AnomalyDetectionIndices anomalyDetectionIndices,
        ClientUtil clientUtil,
        IndexUtils indexUtils,
        ClusterService clusterService
    ) {
        super(
            client,
            settings,
            threadPool,
            AnomalyResult.ANOMALY_RESULT_INDEX,
            ThrowingConsumerWrapper.throwingConsumerWrapper(anomalyDetectionIndices::initAnomalyResultIndexDirectly),
            anomalyDetectionIndices::doesAnomalyResultIndexExist,
            clientUtil,
            indexUtils,
            clusterService
        );
    }

}
