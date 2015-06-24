package io.reactivex.aeron;

import io.reactivex.aeron.requestreply.RequestReplyClient;
import io.reactivex.aeron.requestreply.RequestReplyServer;
import io.reactivex.aeron.unicast.UnicastClient;
import io.reactivex.aeron.unicast.UnicastServer;
import rx.Observable;
import rx.functions.Func1;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.collections.Long2ObjectHashMap;

/**
 * Created by rroeser on 6/16/15.
 */
public interface RxAeronFactory {
    UnicastClient<DirectBuffer> createUnicastClient(String channel);

    <T>  UnicastClient<T> createUnicastClient(String channel, PublicationDataHandler<T> dataHandler);

    UnicastServer createUnicastServer(String channel, Func1<Observable<DirectBuffer>, Observable<?>> handle);

    UnicastServer createUnicastServer(String channel, Long2ObjectHashMap<SubscriptionDataHandler> handlers);

    RequestReplyClient createRequestReplyClient(String serverChannel, String responseChannel);

    RequestReplyServer createRequestReplyServer(String channel, Func1<Observable<DirectBuffer>, Observable<DirectBuffer>> handle);
}
