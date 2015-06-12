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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import rx.Subscriber;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.LangUtil;
import uk.co.real_logic.agrona.MutableDirectBuffer;
import uk.co.real_logic.agrona.collections.Long2ObjectHashMap;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.io.IOException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by rroeser on 6/8/15.
 */
public class DefaultRequestReplyClient implements RequestReplyClient {
    private static final Logger logger = LoggerFactory.getLogger(DefaultRequestReplyClient.class);

    private final RxAeron rxAeron;

    private final Long2ObjectHashMap<Subscriber<? super DirectBuffer>> transactionIdToResponse;

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
        if (!initialized) {
            establishConnection(serverChannel, responseChannel);
        }

        final AtomicLong countSent = new AtomicLong(-1);
        final AtomicLong countReceived = new AtomicLong(0);

        return Observable.<DirectBuffer>create(s -> {
            long transactionId = TransactionIdUtil.getTransactionId();
            transactionIdToResponse.put(transactionId, s);

            client.offer(buffer.map(b ->
                new Request(transactionId, b)))
                .countLong()
                .doOnNext(countSent::set)
                .subscribe();
        })
        .takeUntil(b -> {
            long sent = countSent.get();
            long received = countReceived.incrementAndGet();

            return sent == received;
        });
    }

    private synchronized void establishConnection(String serverChannel, String responseChannel) {
        if (!initialized) {
            UnicastServer unicastServer = null;
            UnicastClient<String> unicastClient = null;

            try {
                if (logger.isDebugEnabled()) {
                    logger.debug("Not initialized, establishing a connection for server channel " + serverChannel + ", and response channel " + responseChannel);
                }

                final CyclicBarrier barrier = new CyclicBarrier(2);

                SubscriptionDataHandler establishConnectionAckDataHandler = (buffer, offset, length) -> {
                    EstablishConnectionAckDecoder establishConnectionAckDecoder = new EstablishConnectionAckDecoder();
                    establishConnectionAckDecoder.wrap(buffer, offset, length, 0);

                    connectionId = establishConnectionAckDecoder.connectionId();

                    if (logger.isDebugEnabled()) {
                        logger.debug("Received connection id => " + connectionId);
                    }

                    try {
                        barrier.await(15, TimeUnit.SECONDS);
                    } catch (Throwable t) {
                        if (logger.isDebugEnabled()) {
                            logger.debug("Timed awaiting client await");
                        }
                    }

                    return Observable.empty();
                };

                if (logger.isDebugEnabled()) {
                    logger.debug("Creating subscription to listen for connection id for server channel " + serverChannel + ", and response channel " + responseChannel);
                }
                Long2ObjectHashMap<SubscriptionDataHandler> map = new Long2ObjectHashMap<>();
                map.put(EstablishConnectionAckDecoder.TEMPLATE_ID, establishConnectionAckDataHandler);
                unicastServer = rxAeron.createUnicastServer(responseChannel, map);

                if (logger.isDebugEnabled()) {
                    logger.debug("Creating client to request connection id for server channel " + serverChannel + ", and response channel " + responseChannel);
                }
                unicastClient = rxAeron.createUnicastClient(serverChannel, new PublicationDataHandler<String>() {
                    @Override
                    public DirectBuffer call(MutableDirectBuffer requestBuffer, Integer offset, String s) {
                        EstablishConnectionEncoder establishConnectionEncoder = new EstablishConnectionEncoder();
                        establishConnectionEncoder.wrap(requestBuffer, offset);

                        establishConnectionEncoder.responseChannel(s);

                        return requestBuffer;
                    }

                    @Override
                    public int getBlockLength() {
                        return EstablishConnectionEncoder.BLOCK_LENGTH;
                    }

                    @Override
                    public int getTemplateId() {
                        return EstablishConnectionEncoder.TEMPLATE_ID;
                    }

                    @Override
                    public int getSchemaId() {
                        return EstablishConnectionEncoder.SCHEMA_ID;
                    }

                    @Override
                    public int getSchemaVersion() {
                        return EstablishConnectionEncoder.SCHEMA_VERSION;
                    }
                });

                if (logger.isDebugEnabled()) {
                    logger.debug("Requesting connection id for server channel " + serverChannel + ", and response channel " + responseChannel);
                }
                Observable<Void> offer = unicastClient.offer(Observable.just(responseChannel));
                offer.subscribe();

                if (logger.isDebugEnabled()) {
                    logger.debug("Waiting for connection id for server channel " + serverChannel + ", and response channel " + responseChannel);
                }

                try {
                    barrier.await(15, TimeUnit.SECONDS);
                } catch (TimeoutException e) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Timed out waiting response from server");
                    }
                    LangUtil.rethrowUnchecked(e);
                } catch (Exception e) {
                    LangUtil.rethrowUnchecked(e);
                }

                if (logger.isDebugEnabled()) {
                    logger.debug("Creating subscription to handle responses for connection id " + connectionId);
                }
                map = new Long2ObjectHashMap<>();
                map.put(ServerResponseDecoder.TEMPLATE_ID, (buffer, offset, length) -> {
                        ServerResponseDecoder serverResponseDecoder = new ServerResponseDecoder();
                        serverResponseDecoder.wrap(buffer, offset, length, 0);

                        long transactionId = serverResponseDecoder.transctionId();
                        byte[] bytes = new byte[serverResponseDecoder.payloadLength()];
                        serverResponseDecoder.getPayload(bytes, 0, serverResponseDecoder.payloadLength());

                        if (logger.isDebugEnabled()) {
                            logger.debug("Handling response for transaction id " + transactionId);
                        }

                        UnsafeBuffer payloadBuffer = new UnsafeBuffer(bytes);

                        Subscriber<? super DirectBuffer> subscriber = transactionIdToResponse.get(transactionId);

                        if (subscriber == null) {
                            return Observable.error(new IllegalStateException("No transaction found for transaction id " + transactionId));
                        } else {

                            subscriber.onNext(payloadBuffer);

                            return Observable.empty();
                        }
                    }
                );

                responseServer = rxAeron.createUnicastServer(responseChannel, map);

                if (logger.isDebugEnabled()) {
                    logger.debug("Establishing connection for connection id " + connectionId + ", server channel " + serverChannel + ", and response channel " + responseChannel);
                }
                client = rxAeron.createUnicastClient(serverChannel, new PublicationDataHandler<Request>() {
                    final long cid = connectionId;

                    @Override
                    public DirectBuffer call(MutableDirectBuffer requestBuffer, Integer offset, Request request) {
                        ClientRequestEncoder clientRequestEncoder = new ClientRequestEncoder();
                        clientRequestEncoder.wrap(requestBuffer, offset);

                        long transactionId = request.getTransactionId();

                        if (logger.isDebugEnabled()) {
                            logger.debug("Sending request for transaction id " + transactionId);
                        }

                        clientRequestEncoder.transactionId(transactionId);
                        clientRequestEncoder.connectionId(cid);
                        clientRequestEncoder.putPayload(request.getPayload(), 0, request.getPayload().capacity());

                        return requestBuffer;
                    }

                    @Override
                    public int getBlockLength() {
                        return ClientRequestEncoder.BLOCK_LENGTH;
                    }

                    @Override
                    public int getTemplateId() {
                        return ClientRequestEncoder.TEMPLATE_ID;
                    }

                    @Override
                    public int getSchemaId() {
                        return ClientRequestEncoder.SCHEMA_ID;
                    }

                    @Override
                    public int getSchemaVersion() {
                        return ClientRequestEncoder.SCHEMA_VERSION;
                    }
                });

                if (logger.isDebugEnabled()) {
                    logger.debug("Connection for connection id " + connectionId + " successfully established");
                }
                initialized = true;

            }
            finally {
                try {
                    unicastClient.close();
                } catch (Exception e1) {

                }

                try {
                    unicastServer.close();
                } catch (Exception e1) {

                }
            }
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
