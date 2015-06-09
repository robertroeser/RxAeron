package io.reactivex.aeron.unicast;

import io.reactivex.aeron.protocol.UnicastRequestDecoder;
import rx.Observable;
import rx.functions.Func1;
import rx.functions.Func3;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

/**
 * Created by rroeser on 6/9/15.
 */
    public class UnicastRequestDataHandler implements Func3<DirectBuffer, Integer, Integer, Observable<Void>> {

    private final UnicastRequestDecoder unicastRequestDecoder = new UnicastRequestDecoder();
    private final Func1<Observable<DirectBuffer>, Observable<Void>> handle;

    public UnicastRequestDataHandler(Func1<Observable<DirectBuffer>, Observable<Void>> handle) {
        this.handle = handle;
    }

    @Override
    public Observable<Void> call(DirectBuffer buffer, Integer offset, Integer length) {
        unicastRequestDecoder.wrap(buffer, offset, length, 0);

        byte[] bytes = new byte[unicastRequestDecoder.payloadLength()];
        unicastRequestDecoder.getPayload(bytes, 0, unicastRequestDecoder.payloadLength());

        UnsafeBuffer payloadBuffer = new UnsafeBuffer(bytes);
        Observable<DirectBuffer> dataObservable = Observable.just(payloadBuffer);
        Observable<Void> handleObservable = handle.call(dataObservable);

        return handleObservable;
    }
}
