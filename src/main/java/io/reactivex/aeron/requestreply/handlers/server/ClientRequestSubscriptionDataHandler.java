package io.reactivex.aeron.requestreply.handlers.server;

import io.reactivex.aeron.SubscriptionDataHandler;
import io.reactivex.aeron.protocol.ClientRequestDecoder;
import io.reactivex.aeron.protocol.MessageHeaderEncoder;
import io.reactivex.aeron.protocol.ServerResponseEncoder;
import io.reactivex.aeron.protocol.UnicastRequestEncoder;
import io.reactivex.aeron.unicast.UnicastClient;
import rx.Observable;
import rx.functions.Func1;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.collections.Long2ObjectHashMap;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;

/**
 * Created by rroeser on 6/9/15.
 */
public class ClientRequestSubscriptionDataHandler implements SubscriptionDataHandler {
    private final Func1<Observable<DirectBuffer>, Observable<DirectBuffer>> handle;
    private final Long2ObjectHashMap<UnicastClient> serverResponseClients;

    private final ClientRequestDecoder clientRequestDecoder = new ClientRequestDecoder();

    public ClientRequestSubscriptionDataHandler(Func1<Observable<DirectBuffer>, Observable<DirectBuffer>> handle, Long2ObjectHashMap<UnicastClient> serverResponseClients) {
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

        UnsafeBuffer payloadBuffer = new UnsafeBuffer(bytes);
        Observable<DirectBuffer> dataObservable = Observable.just(payloadBuffer);
        Observable<DirectBuffer> result = handle.call(dataObservable);

        return result.flatMap(r -> {
            UnicastClient<DirectBuffer> unicastClient = serverResponseClients.get(connectionId);

            if (unicastClient == null) {
                return Observable.error(new IllegalStateException("unknown connection id " + connectionId));
            } else {
                UnsafeBuffer requestBuffer = new UnsafeBuffer(ByteBuffer.allocate(1024));
                MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();

                messageHeaderEncoder.wrap(requestBuffer, 0, 0);

                messageHeaderEncoder
                    .blockLength(UnicastRequestEncoder.BLOCK_LENGTH)
                    .templateId(UnicastRequestEncoder.TEMPLATE_ID)
                    .schemaId(UnicastRequestEncoder.SCHEMA_ID)
                    .version(UnicastRequestEncoder.SCHEMA_VERSION);

                ServerResponseEncoder serverResponseEncoder = new ServerResponseEncoder();
                serverResponseEncoder.transctionId(transactionId);
                serverResponseEncoder.putPayload(r, 0, r.capacity());
                return unicastClient.offer(Observable.just(r));
            }
        });
    }
}
