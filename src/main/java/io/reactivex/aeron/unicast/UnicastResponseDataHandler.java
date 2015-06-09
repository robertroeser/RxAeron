package io.reactivex.aeron.unicast;

import io.reactivex.aeron.protocol.UnicastRequestEncoder;
import rx.functions.Func3;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.MutableDirectBuffer;

/**
 * Created by rroeser on 6/9/15.
 */
public class UnicastResponseDataHandler implements Func3<MutableDirectBuffer, Integer, DirectBuffer, DirectBuffer> {
    @Override
    public DirectBuffer call(MutableDirectBuffer requestBuffer, Integer offset, DirectBuffer payload) {
        UnicastRequestEncoder unicastRequestEncoder = new UnicastRequestEncoder();
        unicastRequestEncoder.wrap(requestBuffer, offset);

        unicastRequestEncoder.putPayload(payload, 0, payload.capacity());

        return requestBuffer;
    }
}
