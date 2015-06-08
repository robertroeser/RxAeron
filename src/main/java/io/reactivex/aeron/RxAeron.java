package io.reactivex.aeron;

import io.reactivex.aeron.unicast.DefaultUnicastClient;
import io.reactivex.aeron.unicast.DefaultUnicastServer;
import io.reactivex.aeron.unicast.UnicastClient;
import io.reactivex.aeron.unicast.UnicastServer;
import rx.Observable;
import rx.functions.Func1;
import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.driver.MediaDriver;
import uk.co.real_logic.agrona.DirectBuffer;

import java.io.Closeable;
import java.io.IOException;

/**
 * Created by rroeser on 6/5/15.
 */
public class RxAeron implements Closeable {

    private static RxAeron instance;

    protected final Aeron aeron;
    private final Aeron.Context context;

    private final MediaDriver.Context ctx;
    private final MediaDriver mediaDriver;

    private static final int UNICAST_STREAM_ID = 1;
    private static final int MULTICAST_STREAM_ID = 2;

    private RxAeron() {
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

    public static RxAeron getInstance() {
        if (instance == null) {
            init();
        }

        return instance;
    }

    private synchronized static void init() {
        if (instance == null) {
            instance = new RxAeron();
        }
    }

    UnicastClient createUnicastClient(String channel) {
        return new DefaultUnicastClient(aeron, channel, UNICAST_STREAM_ID);
    }

    UnicastServer createUnicastServer(String channel, Func1<Observable<DirectBuffer>, Observable<Void>> handle) {
        return new DefaultUnicastServer(aeron, channel, UNICAST_STREAM_ID, handle);
    }

    @Override
    public void close() throws IOException {
        aeron.close();
        mediaDriver.close();
    }
}
