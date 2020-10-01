package com.amazon.opendistroforelasticsearch.ad;

/**
 * Represent a state organized via detectorId.  When deleting a detector's state,
 * we can remove it from the state.
 *
 *
 */
public interface CleanState {
    /**
     * Remove state associated with a detector Id
     * @param detectorId Detector Id
     */
    void clear(String detectorId);
}
