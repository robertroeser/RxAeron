package io.reactivex.aeron.requestreply.handlers.server;

import io.reactivex.aeron.PublicationDataHandler;
import io.reactivex.aeron.protocol.EstablishConnectionAckEncoder;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.MutableDirectBuffer;

/**
 * Created by rroeser on 6/9/15.
 */
public class EstablishConnectionAckServerDataHandler implements PublicationDataHandler<Long> {
    @Override
    public DirectBuffer call(MutableDirectBuffer requestBuffer, Integer offset, Long connectionId) {
        EstablishConnectionAckEncoder establishConnectionAckEncoder = new EstablishConnectionAckEncoder();
        establishConnectionAckEncoder.wrap(requestBuffer, offset);
        establishConnectionAckEncoder.connectionId(connectionId);

        return requestBuffer;
    }
}
