package io.reactivex.aeron;

import io.reactivex.aeron.requestreply.RequestReplyClient;
import io.reactivex.aeron.requestreply.RequestReplyServer;
import io.reactivex.aeron.unicast.UnicastClient;
import io.reactivex.aeron.unicast.UnicastServer;
import rx.Observable;
import rx.functions.Func1;
import uk.co.real_logic.agrona.DirectBuffer;

/**
 * Created by rroeser on 6/5/15.
 */
public class RxAeron {

    public static UnicastClient<DirectBuffer> createUnicastClient(String channel) {
        return RxAeronFactoryImpl.getInstance().createUnicastClient(channel);
    }

    public static UnicastServer createUnicastServer(String channel, Func1<Observable<DirectBuffer>, Observable<?>> handle) {
        return RxAeronFactoryImpl.getInstance().createUnicastServer(channel, handle);
    }

    public static RequestReplyClient createRequestReplyClient(String serverChannel, String responseChannel) {
        return RxAeronFactoryImpl.getInstance().createRequestReplyClient(serverChannel, responseChannel);
    }

    public static RequestReplyServer createRequestReplyServer(String channel, Func1<Observable<DirectBuffer>, Observable<DirectBuffer>> handle) {
        return RxAeronFactoryImpl.getInstance().createRequestReplyServer(channel, handle);
    }

}
