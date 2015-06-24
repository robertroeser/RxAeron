package io.reactivex.aeron.requestreply.handlers.server;

import io.reactivex.aeron.PublicationDataHandler;
import io.reactivex.aeron.protocol.EstablishConnectionAckEncoder;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.MutableDirectBuffer;

/**
 * Created by rroeser on 6/9/15.
 */
public class EstablishConnectionAckServerDataHandler implements PublicationDataHandler<Long> {
    private final EstablishConnectionAckEncoder establishConnectionAckEncoder = new EstablishConnectionAckEncoder();

    @Override
    public DirectBuffer call(MutableDirectBuffer requestBuffer, Integer offset, Long connectionId) {

        establishConnectionAckEncoder.wrap(requestBuffer, offset);
        establishConnectionAckEncoder.connectionId(connectionId);

        return requestBuffer;
    }

    @Override
    public int getBlockLength() {
        return EstablishConnectionAckEncoder.BLOCK_LENGTH;
    }

    @Override
    public int getTemplateId() {
        return EstablishConnectionAckEncoder.TEMPLATE_ID;
    }

    @Override
    public int getSchemaId() {
        return EstablishConnectionAckEncoder.SCHEMA_ID;
    }

    @Override
    public int getSchemaVersion() {
        return EstablishConnectionAckEncoder.SCHEMA_VERSION;
    }
}
