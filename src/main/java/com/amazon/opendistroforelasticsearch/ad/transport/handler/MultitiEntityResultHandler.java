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

import java.time.Clock;
import java.util.Locale;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.threadpool.ThreadPool;

import com.amazon.opendistroforelasticsearch.ad.NodeStateManager;
import com.amazon.opendistroforelasticsearch.ad.common.exception.AnomalyDetectionException;
import com.amazon.opendistroforelasticsearch.ad.constant.CommonName;
import com.amazon.opendistroforelasticsearch.ad.indices.AnomalyDetectionIndices;
import com.amazon.opendistroforelasticsearch.ad.model.AnomalyResult;
import com.amazon.opendistroforelasticsearch.ad.transport.AdaptiveBulkAction;
import com.amazon.opendistroforelasticsearch.ad.transport.AdaptiveBulkRequest;
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
    private static final Logger LOG = LogManager.getLogger(MultitiEntityResultHandler.class);
    private final NodeStateManager nodeStateManager;
    private final Clock clock;

    @Inject
    public MultitiEntityResultHandler(
        Client client,
        Settings settings,
        ThreadPool threadPool,
        AnomalyDetectionIndices anomalyDetectionIndices,
        ClientUtil clientUtil,
        IndexUtils indexUtils,
        ClusterService clusterService,
        NodeStateManager nodeStateManager,
        Clock clock
    ) {
        super(
            client,
            settings,
            threadPool,
            CommonName.MULTI_ENTITY_ANOMALY_RESULT_INDEX_ALIAS,
            ThrowingConsumerWrapper.throwingConsumerWrapper(anomalyDetectionIndices::initMultiEntityAnomalyResultIndexDirectly),
            anomalyDetectionIndices::doesMultiEntityAnomalyResultIndexExist,
            clientUtil,
            indexUtils,
            clusterService
        );
        this.nodeStateManager = nodeStateManager;
        this.clock = clock;
    }

    public void write(AnomalyResult toSave, AdaptiveBulkRequest currentBulkRequest) {
        currentBulkRequest.add(toSave);
    }

    public void flush(AdaptiveBulkRequest currentBulkRequest, String detectorId) {
        if (indexUtils.checkIndicesBlocked(clusterService.state(), ClusterBlockLevel.WRITE, this.indexName)) {
            LOG.warn(String.format(Locale.ROOT, CANNOT_SAVE_ERR_MSG, detectorId));
            return;
        }

        try {
            if (!indexExists.getAsBoolean()) {
                createIndex
                    .accept(
                        ActionListener
                            .wrap(initResponse -> onCreateIndexResponse(initResponse, currentBulkRequest, detectorId), exception -> {
                                if (ExceptionsHelper.unwrapCause(exception) instanceof ResourceAlreadyExistsException) {
                                    // It is possible the index has been created while we sending the create request
                                    bulk(currentBulkRequest, detectorId);
                                } else {
                                    throw new AnomalyDetectionException(
                                        detectorId,
                                        String.format("Unexpected error creating index %s", indexName),
                                        exception
                                    );
                                }
                            })
                    );
            } else {
                bulk(currentBulkRequest, detectorId);
            }
        } catch (Exception e) {
            throw new AnomalyDetectionException(
                detectorId,
                String.format(Locale.ROOT, "Error in bulking %s for detector %s", indexName, detectorId),
                e
            );
        }
    }

    private void onCreateIndexResponse(CreateIndexResponse response, AdaptiveBulkRequest bulkRequest, String detectorId) {
        if (response.isAcknowledged()) {
            bulk(bulkRequest, detectorId);
        } else {
            throw new AnomalyDetectionException(detectorId, "Creating %s with mappings call not acknowledged.");
        }
    }

    private void bulk(AdaptiveBulkRequest currentBulkRequest, String detectorId) {
        if (currentBulkRequest.numberOfActions() <= 0) {
            return;
        }
        client
            .execute(
                AdaptiveBulkAction.INSTANCE,
                currentBulkRequest,
                ActionListener
                    .<BulkResponse>wrap(
                        response -> LOG.debug(String.format(SUCCESS_SAVING_MSG, detectorId)),
                        exception -> {
                            LOG.error(String.format(FAIL_TO_SAVE_ERR_MSG, detectorId), exception);
                            // too much indexing pressure
                            // TODO: pause indexing a bit before trying again, ideally with randomized exponential backoff.
                            if (exception instanceof EsRejectedExecutionException) {
                                nodeStateManager.setLastIndexThrottledTime(clock.instant());
                            }
                        }
                    )
            );
    }
}
