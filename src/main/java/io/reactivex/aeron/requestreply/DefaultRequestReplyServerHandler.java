package io.reactivex.aeron.requestreply;

import rx.Observable;
import rx.functions.Func1;
import uk.co.real_logic.agrona.DirectBuffer;

import java.io.Closeable;
import java.io.IOException;

/**
 * Created by rroeser on 6/9/15.
 */
public class DefaultRequestReplyServerHandler implements Func1<Observable<DirectBuffer>, Observable<Void>>, Closeable {
    private Func1<Observable<DirectBuffer>, Observable<DirectBuffer>> handle;

    @Override
    public Observable<Void> call(Observable<DirectBuffer> bufferObservable) {
        return null;
    }

    @Override
    public void close() throws IOException {

    }
}
