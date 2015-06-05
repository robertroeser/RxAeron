package io.reactivex.aeron.unicast;

import io.reactivex.aeron.operators.OperatorPublish;
import io.reactivex.aeron.protocol.MessageHeaderEncoder;
import io.reactivex.aeron.protocol.UnicastRequestEncoder;
import rx.Observable;
import rx.schedulers.Schedulers;
import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by rroeser on 6/5/15.
 */
public class DefaultUnicastClient implements UnicastClient {
    private final Publication publication;
    private final String channel;
    private final int streamId;
    private final OperatorPublish operatorPublish;

    public DefaultUnicastClient(Aeron aeron, String channel, int streamId) {
        this.channel = channel;
        this.streamId = streamId;
        this.publication = aeron.addPublication(channel, streamId);

        this.operatorPublish = new OperatorPublish(Schedulers.computation(), publication);
    }

    @Override
    public Observable<Void> offer(Observable<DirectBuffer> buffer) {

        buffer
            .map(b -> {
                UnsafeBuffer requestBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(1024));
                MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
                UnicastRequestEncoder unicastRequestEncoder = new UnicastRequestEncoder();

                messageHeaderEncoder.wrap(requestBuffer, 0, 0);

                messageHeaderEncoder
                    .blockLength(UnicastRequestEncoder.BLOCK_LENGTH)
                    .templateId(UnicastRequestEncoder.TEMPLATE_ID)
                    .schemaId(UnicastRequestEncoder.SCHEMA_ID)
                    .version(UnicastRequestEncoder.SCHEMA_VERSION);

                unicastRequestEncoder.wrap(requestBuffer, messageHeaderEncoder.size());

                unicastRequestEncoder.putPayload(b, 0, b.byteArray().length);

                return requestBuffer;
            })
            .lift(operatorPublish);

        return null;
    }

    public int getStreamId() {
        return streamId;
    }

    public String getChannel() {
        return channel;
    }

    @Override
    public void close() throws IOException {
        publication.close();
    }
}
