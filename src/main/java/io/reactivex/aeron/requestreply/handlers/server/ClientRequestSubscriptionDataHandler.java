package io.reactivex.aeron.requestreply.handlers.server;

import io.reactivex.aeron.SubscriptionDataHandler;
import io.reactivex.aeron.protocol.ClientRequestDecoder;
import io.reactivex.aeron.unicast.UnicastClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.functions.Func1;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.collections.Long2ObjectHashMap;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

/**
 * Created by rroeser on 6/9/15.
 */
public class ClientRequestSubscriptionDataHandler implements SubscriptionDataHandler {
    private static final Logger logger = LoggerFactory.getLogger(ClientRequestSubscriptionDataHandler.class);

    private final Func1<Observable<DirectBuffer>, Observable<DirectBuffer>> handle;
    private final Long2ObjectHashMap<UnicastClient<Response>> serverResponseClients;

    private final ClientRequestDecoder clientRequestDecoder = new ClientRequestDecoder();

    public ClientRequestSubscriptionDataHandler(Func1<Observable<DirectBuffer>, Observable<DirectBuffer>> handle, Long2ObjectHashMap<UnicastClient<Response>> serverResponseClients) {
        this.handle = handle;
        this.serverResponseClients = serverResponseClients;
    }

    @Override
    public Observable<Void> call(DirectBuffer buffer, Integer offset, Integer length) {
        clientRequestDecoder.wrap(buffer, offset, length, 0);

        byte[] bytes = new byte[clientRequestDecoder.payloadLength()];
        clientRequestDecoder.getPayload(bytes, 0, clientRequestDecoder.payloadLength());
        long transactionId = clientRequestDecoder.transactionId();
        long connectionId = clientRequestDecoder.connectionId();

        if (logger.isDebugEnabled()) {
            logger.debug("Server handling client request for transaction id " + transactionId + ", and connection id " + connectionId);
        }

        UnsafeBuffer payloadBuffer = new UnsafeBuffer(bytes);
        Observable<DirectBuffer> dataObservable = Observable.just(payloadBuffer);
        Observable<DirectBuffer> result = handle.call(dataObservable);

        UnicastClient<Response> unicastClient = serverResponseClients.get(connectionId);

        if (unicastClient == null) {
            return Observable.error(new IllegalStateException("unknown connection id " + connectionId));
        } else {
            return unicastClient.offer(result.map(r -> new Response(transactionId, r)));
        }
    }
}
