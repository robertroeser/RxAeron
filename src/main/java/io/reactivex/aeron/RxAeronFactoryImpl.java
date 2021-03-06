package io.reactivex.aeron;

import io.reactivex.aeron.protocol.ClientRequestDecoder;
import io.reactivex.aeron.protocol.EstablishConnectionDecoder;
import io.reactivex.aeron.protocol.UnicastRequestDecoder;
import io.reactivex.aeron.requestreply.DefaultRequestReplyClient;
import io.reactivex.aeron.requestreply.DefaultRequestReplyServer;
import io.reactivex.aeron.requestreply.RequestReplyClient;
import io.reactivex.aeron.requestreply.RequestReplyServer;
import io.reactivex.aeron.requestreply.handlers.server.ClientRequestSubscriptionDataHandler;
import io.reactivex.aeron.requestreply.handlers.server.EstablishConnectionSubscriptionDataHandler;
import io.reactivex.aeron.requestreply.handlers.server.Response;
import io.reactivex.aeron.unicast.DefaultUnicastClient;
import io.reactivex.aeron.unicast.DefaultUnicastServer;
import io.reactivex.aeron.unicast.UnicastClient;
import io.reactivex.aeron.unicast.UnicastServer;
import io.reactivex.aeron.unicast.handlers.UnicastPublicationDataHandler;
import io.reactivex.aeron.unicast.handlers.UnicastSubscriptionDataHandler;
import rx.Observable;
import rx.functions.Func1;
import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.driver.MediaDriver;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.collections.Long2ObjectHashMap;

import java.io.Closeable;
import java.io.IOException;

/**
 * Created by rroeser on 6/16/15.
 */

class RxAeronFactoryImpl implements Closeable, RxAeronFactory {

    private static RxAeronFactory instance;

    final Aeron aeron;
    private final Aeron.Context context;

    private final MediaDriver.Context ctx;
    private final MediaDriver mediaDriver;

    private static final int UNICAST_STREAM_ID = 1;

    private RxAeronFactoryImpl() {
        ctx = new MediaDriver.Context();
        mediaDriver = MediaDriver.launch(ctx.dirsDeleteOnExit(true));

        context = new Aeron.Context()
            .newConnectionHandler((String channel, int streamId, int sessionId, long joiningPosition, String sourceInformation) ->
                    System.out.println("New Connection => channel: " + channel
                        + " stream: " + streamId
                        + " session: " + sessionId
                        + " position: " + joiningPosition
                        + "  source: " + sourceInformation)
            )
            .errorHandler(Throwable::printStackTrace);

        aeron = Aeron.connect(context);

    }

    static RxAeronFactory getInstance() {
        if (instance == null) {
            init();
        }

        return instance;
    }

    private synchronized static void init() {
        if (instance == null) {
            instance = new RxAeronFactoryImpl();
        }
    }

    @Override
    public UnicastClient<DirectBuffer> createUnicastClient(String channel) {
        return createUnicastClient(channel, new UnicastPublicationDataHandler());
    }

    @Override
    public <T>  UnicastClient<T> createUnicastClient(String channel, PublicationDataHandler<T> dataHandler) {
        return new DefaultUnicastClient<>(
            aeron,
            channel,
            UNICAST_STREAM_ID,
            dataHandler);
    }

    @Override
    public UnicastServer createUnicastServer(String channel, Func1<Observable<DirectBuffer>, Observable<?>> handle) {
        UnicastSubscriptionDataHandler unicastServerDataHandler = new UnicastSubscriptionDataHandler(handle);
        Long2ObjectHashMap<SubscriptionDataHandler> handlers
            = new Long2ObjectHashMap<>();
        handlers.put(UnicastRequestDecoder.TEMPLATE_ID, unicastServerDataHandler);

        return createUnicastServer(channel, handlers);
    }

    @Override
    public UnicastServer createUnicastServer(String channel, Long2ObjectHashMap<SubscriptionDataHandler> handlers) {
        return new DefaultUnicastServer(aeron, channel, UNICAST_STREAM_ID, handlers);
    }

    @Override
    public RequestReplyClient createRequestReplyClient(String serverChannel, String responseChannel) {
        return new DefaultRequestReplyClient(this, serverChannel, responseChannel);
    }

    @Override
    public RequestReplyServer createRequestReplyServer(String channel, Func1<Observable<DirectBuffer>, Observable<DirectBuffer>> handle) {

        Long2ObjectHashMap<UnicastClient<Response>> serverResponseClients = new Long2ObjectHashMap<>();

        EstablishConnectionSubscriptionDataHandler establishConnectionServerDataHandler
            = new EstablishConnectionSubscriptionDataHandler(serverResponseClients, this);

        Long2ObjectHashMap<SubscriptionDataHandler> handlers
            = new Long2ObjectHashMap<>();

        handlers.put(EstablishConnectionDecoder.TEMPLATE_ID, establishConnectionServerDataHandler);

        ClientRequestSubscriptionDataHandler clientRequestServerDataHandler = new ClientRequestSubscriptionDataHandler(handle, serverResponseClients);

        handlers.put(ClientRequestDecoder.TEMPLATE_ID, clientRequestServerDataHandler);

        DefaultUnicastServer defaultUnicastServer = new DefaultUnicastServer(aeron, channel, UNICAST_STREAM_ID, handlers);

        return new DefaultRequestReplyServer(defaultUnicastServer);
    }

    @Override
    public void close() throws IOException {
        aeron.close();
        mediaDriver.close();
    }
}