package io.reactivex.aeron.requestreply.handlers.client;

import uk.co.real_logic.agrona.DirectBuffer;

/**
* Created by rroeser on 6/16/15.
*/
public class Request {
    private long transactionId;
    private DirectBuffer payload;

    public Request(long transactionId, DirectBuffer payload) {
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
