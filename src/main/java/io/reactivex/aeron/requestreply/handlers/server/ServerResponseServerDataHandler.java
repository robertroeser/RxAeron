package io.reactivex.aeron.requestreply.handlers.server;

import io.reactivex.aeron.PublicationDataHandler;
import io.reactivex.aeron.protocol.ServerResponseEncoder;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.MutableDirectBuffer;

/**
 * Created by rroeser on 6/9/15.
 */
public class ServerResponseServerDataHandler implements PublicationDataHandler<Response> {
    private ServerResponseEncoder serverResponseEncoder = new ServerResponseEncoder();

    @Override
    public DirectBuffer call(MutableDirectBuffer requestBuffer, Integer offset, Response response) {
        serverResponseEncoder.wrap(requestBuffer, offset);

        serverResponseEncoder.transctionId(response.getTransactionId());
        serverResponseEncoder.putPayload(response.getPayload(), 0, response.getPayload().capacity());

        return requestBuffer;
    }

    @Override
    public int getBlockLength() {
        return ServerResponseEncoder.BLOCK_LENGTH;
    }

    @Override
    public int getTemplateId() {
        return ServerResponseEncoder.TEMPLATE_ID;
    }

    @Override
    public int getSchemaId() {
        return ServerResponseEncoder.SCHEMA_ID;
    }

    @Override
    public int getSchemaVersion() {
        return ServerResponseEncoder.SCHEMA_VERSION;
    }
}
