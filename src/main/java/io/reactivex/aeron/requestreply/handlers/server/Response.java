package io.reactivex.aeron.requestreply.handlers.server;

import uk.co.real_logic.agrona.DirectBuffer;

/**
 * Created by rroeser on 6/12/15.
 */
public class Response {
    private long transactionId;
    private DirectBuffer payload;

    public Response(long transactionId, DirectBuffer payload) {
        this.transactionId = transactionId;
        this.payload = payload;
    }

    public long getTransactionId() {
        return transactionId;
    }

    public DirectBuffer getPayload() {
        return payload;
    }
}
