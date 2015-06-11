package io.reactivex.aeron.unicast.handlers;

import io.reactivex.aeron.PublicationDataHandler;
import io.reactivex.aeron.protocol.UnicastRequestEncoder;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.MutableDirectBuffer;

/**
 * Created by rroeser on 6/9/15.
 */
public class UnicastPublicationDataHandler implements PublicationDataHandler<DirectBuffer> {
    @Override
    public DirectBuffer call(MutableDirectBuffer requestBuffer, Integer offset, DirectBuffer payload) {
        UnicastRequestEncoder unicastRequestEncoder = new UnicastRequestEncoder();
        unicastRequestEncoder.wrap(requestBuffer, offset);

        unicastRequestEncoder.putPayload(payload, 0, payload.capacity());

        return requestBuffer;
    }
}
