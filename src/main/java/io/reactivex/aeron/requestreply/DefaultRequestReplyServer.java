package io.reactivex.aeron.requestreply;

import io.reactivex.aeron.unicast.DefaultUnicastServer;

import java.io.IOException;

/**
 * Created by rroeser on 6/8/15.
 */
public class DefaultRequestReplyServer implements RequestReplyServer {
    private final DefaultUnicastServer server;

    public DefaultRequestReplyServer(DefaultUnicastServer server) {
        this.server = server;
    }

    @Override
    public void close() throws IOException {
        this.server.close();
    }

    @Override
    public void enableRateReport() {
        this.server.enableRateReport();
    }
}
