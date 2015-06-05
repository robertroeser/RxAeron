package io.reactivex.aeron.unicast;

import rx.Observable;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.io.Closeable;

/**
 * Created by rroeser on 6/5/15.
 */
public interface UnicastClient extends Closeable {
    Observable<Void> offer(Observable<DirectBuffer> buffer);

    default DirectBuffer fromBytes(byte[] bytes) {
        return new UnsafeBuffer(bytes);
    }
}
