package io.reactivex.aeron.requestreply.handlers.server;

import io.reactivex.aeron.RxAeron;
import io.reactivex.aeron.SubscriptionDataHandler;
import io.reactivex.aeron.TransactionIdUtil;
import io.reactivex.aeron.protocol.EstablishConnectionDecoder;
import io.reactivex.aeron.unicast.UnicastClient;
import rx.Observable;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.collections.Long2ObjectHashMap;

/**
 * Created by rroeser on 6/9/15.
 */
public class EstablishConnectionSubscriptionDataHandler implements SubscriptionDataHandler {
    private final Long2ObjectHashMap<UnicastClient> serverResponseClients;
    private final RxAeron rxAeron;
    private final EstablishConnectionDecoder establishConnectionDecoder = new EstablishConnectionDecoder();


    public EstablishConnectionSubscriptionDataHandler(Long2ObjectHashMap<UnicastClient> serverResponseClients, RxAeron rxAeron) {
        this.serverResponseClients = serverResponseClients;
        this.rxAeron = rxAeron;
    }

    @Override
    public Observable<Void> call(DirectBuffer buffer, Integer offset, Integer length) {
        establishConnectionDecoder.wrap(buffer, offset, length, 0);

        String responseChannel = establishConnectionDecoder.responseChannel();
        long key = TransactionIdUtil.getConnectionId(responseChannel);

        if (!serverResponseClients.containsKey(key)) {
            try (UnicastClient<Long> ackClient = rxAeron.createUnicastClient(responseChannel, new EstablishConnectionAckServerDataHandler())) {
                return ackClient
                    .offer(Observable.just(key))
                    .doOnNext(f ->
                        serverResponseClients.computeIfAbsent(key, k -> rxAeron.createUnicastClient(responseChannel, new ServerResponseServerDataHandler()))
                    )
                    .map(f -> null);
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }

        return Observable.empty();
    }

}
