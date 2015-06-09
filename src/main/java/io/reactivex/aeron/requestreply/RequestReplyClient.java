package io.reactivex.aeron.requestreply;

import rx.Observable;
import uk.co.real_logic.agrona.DirectBuffer;

import java.io.Closeable;

/**
 * Created by rroeser on 6/8/15.
 */
public interface RequestReplyClient extends Closeable {
    Observable<DirectBuffer> offer(Observable<DirectBuffer> buffer);
}
