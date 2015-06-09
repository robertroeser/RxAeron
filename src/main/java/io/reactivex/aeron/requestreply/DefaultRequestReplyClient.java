package io.reactivex.aeron.requestreply;

import rx.Observable;
import uk.co.real_logic.agrona.DirectBuffer;

import java.io.IOException;

/**
 * Created by rroeser on 6/8/15.
 */
public class DefaultRequestReplyClient implements RequestReplyClient {
    @Override
    public Observable<DirectBuffer> offer(Observable<DirectBuffer> buffer) {
        return null;
    }

    @Override
    public void close() throws IOException {

    }
}
