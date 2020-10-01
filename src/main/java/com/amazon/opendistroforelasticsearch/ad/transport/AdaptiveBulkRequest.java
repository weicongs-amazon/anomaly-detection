package com.amazon.opendistroforelasticsearch.ad.transport;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;


import com.amazon.opendistroforelasticsearch.ad.model.AnomalyResult;

public class AdaptiveBulkRequest extends ActionRequest implements Writeable {
    private final List<AnomalyResult> anomalyResults;

    public AdaptiveBulkRequest() {
        anomalyResults = new ArrayList<>();
    }

    public AdaptiveBulkRequest(StreamInput in) throws IOException {
        super(in);
        int size = in.readVInt();
        anomalyResults = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            anomalyResults.add(new AnomalyResult(in));
        }
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (anomalyResults.isEmpty()) {
            validationException = ValidateActions.addValidationError("no requests added", validationException);
        }
        return validationException;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(anomalyResults.size());
        for (AnomalyResult result : anomalyResults) {
            result.writeTo(out);
        }
    }

    /**
     *
     * @return all of the results to send
     */
    public List<AnomalyResult> getAnomalyResults() {
        return anomalyResults;
    }

    /**
     * Add result to send
     * @param result The result
     */
    public void add(AnomalyResult result) {
        anomalyResults.add(result);
    }

    /**
     *
     * @return total index requests
     */
    public int numberOfActions() {
        return anomalyResults.size();
    }
}
