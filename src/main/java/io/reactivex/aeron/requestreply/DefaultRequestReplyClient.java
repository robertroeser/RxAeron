package io.reactivex.aeron.requestreply;

import io.reactivex.aeron.PublicationDataHandler;
import io.reactivex.aeron.RxAeron;
import io.reactivex.aeron.SubscriptionDataHandler;
import io.reactivex.aeron.TransactionIdUtil;
import io.reactivex.aeron.protocol.ClientRequestEncoder;
import io.reactivex.aeron.protocol.EstablishConnectionAckDecoder;
import io.reactivex.aeron.protocol.EstablishConnectionEncoder;
import io.reactivex.aeron.protocol.ServerResponseDecoder;
import io.reactivex.aeron.unicast.UnicastClient;
import io.reactivex.aeron.unicast.UnicastServer;
import rx.Observable;
import rx.Subscriber;
import rx.functions.Func1;
import rx.subjects.PublishSubject;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.LangUtil;
import uk.co.real_logic.agrona.MutableDirectBuffer;
import uk.co.real_logic.agrona.collections.Long2ObjectHashMap;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Created by rroeser on 6/8/15.
 */
public class DefaultRequestReplyClient implements RequestReplyClient {

    private final RxAeron rxAeron;

    private final Long2ObjectHashMap<PublishSubject> transactionIdToResponse;

    private final String serverChannel;

    private final String responseChannel;

    private volatile long connectionId;

    private UnicastClient<Request> client;

    private UnicastServer responseServer;

    private volatile boolean initialized = false;

    public DefaultRequestReplyClient(RxAeron rxAeron, String serverChannel, String responseChannel) {
        this.rxAeron = rxAeron;
        this.serverChannel = serverChannel;
        this.responseChannel = responseChannel;
        this.transactionIdToResponse = new Long2ObjectHashMap<>();

    }

    @Override
    public Observable<DirectBuffer> offer(Observable<DirectBuffer> buffer) {
        Observable
            .create(subscriber -> {
                if (!initialized) {
                    establishConnection(serverChannel, responseChannel);
                } else {
                    subscriber.onNext(connectionId);
                }

                subscriber.onCompleted();
            });

        return null;
    }

    private synchronized void establishConnection(String serverChannel, String responseChannel) {
        if (!initialized) {
            UnicastClient<String> unicastClient = rxAeron.createUnicastClient(serverChannel, new PublicationDataHandler<String>() {
                @Override
                public DirectBuffer call(MutableDirectBuffer requestBuffer, Integer offset, String s) {
                    EstablishConnectionEncoder establishConnectionEncoder = new EstablishConnectionEncoder();
                    establishConnectionEncoder.wrap(requestBuffer, offset);

                    establishConnectionEncoder.responseChannel(s);

                    return requestBuffer;
                }
            });

            Observable<Void> offer = unicastClient.offer(Observable.just(responseChannel));
            offer.subscribe();

            final CountDownLatch countDownLatch = new CountDownLatch(1);

            SubscriptionDataHandler establishConnectionAckDataHandler = (buffer,  offset, length) -> {
                    EstablishConnectionAckDecoder establishConnectionAckDecoder = new EstablishConnectionAckDecoder();
                    establishConnectionAckDecoder.wrap(buffer, offset, length, 0);

                    connectionId = establishConnectionAckDecoder.connectionId();

                    countDownLatch.countDown();

                    return Observable.empty();
            };

            Long2ObjectHashMap<SubscriptionDataHandler> map = new Long2ObjectHashMap<>();
            map.put(EstablishConnectionAckDecoder.TEMPLATE_ID, establishConnectionAckDataHandler);
            UnicastServer unicastServer = rxAeron.createUnicastServer(responseChannel, map);

            try {
                countDownLatch.await(2, TimeUnit.SECONDS);
            } catch (Exception e) {
                LangUtil.rethrowUnchecked(e);
            } finally {
                try {
                    unicastClient.close();
                } catch (Exception e1) {

                }

                try {
                    unicastServer.close();
                } catch (Exception e1) {

                }
            }

            client = rxAeron.createUnicastClient(serverChannel, new PublicationDataHandler<Request>() {
                @Override
                public DirectBuffer call(MutableDirectBuffer requestBuffer, Integer offset, Request request) {
                    ClientRequestEncoder clientRequestEncoder = new ClientRequestEncoder();
                    clientRequestEncoder.wrap(requestBuffer, offset);

                    clientRequestEncoder.transactionId(request.getTransactionId());
                    clientRequestEncoder.putPayload(request.getPayload(), 0, request.getPayload().capacity());

                    return requestBuffer;
                }
            });

            map = new Long2ObjectHashMap<>();
            map.put(ServerResponseDecoder.TEMPLATE_ID, new SubscriptionDataHandler() {
                @Override
                public Observable<Void> call(DirectBuffer buffer, Integer offset, Integer length) {
                    return null;
                }
            });

            responseServer = rxAeron.createUnicastServer(responseChannel, map);
        }

    }

    static class Request {
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

    @Override
    public void close() throws IOException {
        client.close();
        responseServer.close();
    }
}
