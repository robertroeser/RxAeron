package io.reactivex.aeron.unicast.handlers;

import io.reactivex.aeron.PublicationDataHandler;
import io.reactivex.aeron.protocol.UnicastRequestEncoder;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.MutableDirectBuffer;

/**
 * Created by rroeser on 6/9/15.
 */
public class UnicastPublicationDataHandler implements PublicationDataHandler<DirectBuffer> {
    private final UnicastRequestEncoder unicastRequestEncoder = new UnicastRequestEncoder();

    @Override
    public DirectBuffer call(MutableDirectBuffer requestBuffer, Integer offset, DirectBuffer payload) {

        unicastRequestEncoder.wrap(requestBuffer, offset);

        unicastRequestEncoder.putPayload(payload, 0, payload.capacity());

        return requestBuffer;
    }

    @Override
    public int getBlockLength() {
        return UnicastRequestEncoder.BLOCK_LENGTH;
    }

    @Override
    public int getTemplateId() {
        return UnicastRequestEncoder.TEMPLATE_ID;
    }

    @Override
    public int getSchemaId() {
        return UnicastRequestEncoder.SCHEMA_ID;
    }

    @Override
    public int getSchemaVersion() {
        return UnicastRequestEncoder.SCHEMA_VERSION;
    }
}
