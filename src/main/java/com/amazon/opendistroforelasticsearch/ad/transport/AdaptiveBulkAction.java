package com.amazon.opendistroforelasticsearch.ad.transport;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.transport.TransportRequestOptions;

public class AdaptiveBulkAction extends ActionType<BulkResponse> {

    public static final AdaptiveBulkAction INSTANCE = new AdaptiveBulkAction();
    public static final String NAME = "cluster:admin/ad/write/bulk";

    private AdaptiveBulkAction() {
        super(NAME, BulkResponse::new);
    }

    @Override
    public TransportRequestOptions transportOptions(Settings settings) {
        return TransportRequestOptions.builder().withType(TransportRequestOptions.Type.BULK).build();
    }
}
