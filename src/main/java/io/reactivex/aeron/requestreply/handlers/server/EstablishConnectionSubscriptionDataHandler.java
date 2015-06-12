package io.reactivex.aeron.requestreply.handlers.server;

import io.reactivex.aeron.RxAeron;
import io.reactivex.aeron.SubscriptionDataHandler;
import io.reactivex.aeron.TransactionIdUtil;
import io.reactivex.aeron.protocol.EstablishConnectionDecoder;
import io.reactivex.aeron.unicast.UnicastClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.collections.Long2ObjectHashMap;

import java.io.IOException;

/**
 * Created by rroeser on 6/9/15.
 */
public class EstablishConnectionSubscriptionDataHandler implements SubscriptionDataHandler {
    private static final Logger logger = LoggerFactory.getLogger(EstablishConnectionSubscriptionDataHandler.class);

    private final Long2ObjectHashMap<UnicastClient<Response>> serverResponseClients;
    private final RxAeron rxAeron;
    private final EstablishConnectionDecoder establishConnectionDecoder = new EstablishConnectionDecoder();


    public EstablishConnectionSubscriptionDataHandler(Long2ObjectHashMap<UnicastClient<Response>> serverResponseClients, RxAeron rxAeron) {
        this.serverResponseClients = serverResponseClients;
        this.rxAeron = rxAeron;
    }

    @Override
    public Observable<Void> call(DirectBuffer buffer, Integer offset, Integer length) {
        establishConnectionDecoder.wrap(buffer, offset, length, 0);

        String responseChannel = establishConnectionDecoder.responseChannel();
        long key = TransactionIdUtil.getConnectionId(responseChannel);

        if (logger.isDebugEnabled()) {
            logger.debug("Server " + responseChannel + " and connection id " + key + " establishing connection");
        }
        if (!serverResponseClients.containsKey(key)) {
            if (logger.isDebugEnabled()) {
                logger.debug("No client found for key " + key);
            }

            UnicastClient<Long> ackClient = rxAeron.createUnicastClient(responseChannel, new EstablishConnectionAckServerDataHandler());

            return ackClient
                .offer(Observable.just(key))
                .doOnCompleted(() -> {
                    if (logger.isDebugEnabled()) {
                        logger.debug("Ack sent for connection key " + key);
                    }
                    try {
                        ackClient.close();
                        if (logger.isDebugEnabled()) {
                            logger.debug("Ack connection closed for " + key);
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                })
                .doOnNext(f ->
                        serverResponseClients.computeIfAbsent(key, k ->
                            rxAeron.createUnicastClient(responseChannel, new ServerResponseServerDataHandler()))
                )
                .map(f -> null);
        }

        return Observable.empty();
    }

}
