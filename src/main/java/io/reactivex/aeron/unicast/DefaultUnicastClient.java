package io.reactivex.aeron.unicast;

import io.reactivex.aeron.operators.OperatorPublish;
import io.reactivex.aeron.protocol.UnicastRequestEncoder;
import io.reactivex.aeron.protocol.MessageHeaderEncoder;
import rx.Observable;
import rx.functions.Func3;
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
public class DefaultUnicastClient<T> implements UnicastClient<T> {
    private final Publication publication;
    private final String channel;
    private final int streamId;
    private final OperatorPublish operatorPublish;
    private final  Func3<DirectBuffer, Integer, T, DirectBuffer> unicastClientDataHandler;

    public DefaultUnicastClient(Aeron aeron, String channel, int streamId,  Func3<DirectBuffer, Integer, T, DirectBuffer> unicastClientDataHandler) {
        this.channel = channel;
        this.streamId = streamId;
        this.unicastClientDataHandler = unicastClientDataHandler;
        this.publication = aeron.addPublication(channel, streamId);

        this.operatorPublish = new OperatorPublish(Schedulers.computation(), publication);
    }

    @Override
    public Observable<Void> offer(Observable<T> buffer) {

        return buffer
            .map(b -> {
                UnsafeBuffer requestBuffer = new UnsafeBuffer(ByteBuffer.allocate(1024));
                MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();

                messageHeaderEncoder.wrap(requestBuffer, 0, 0);

                messageHeaderEncoder
                    .blockLength(UnicastRequestEncoder.BLOCK_LENGTH)
                    .templateId(UnicastRequestEncoder.TEMPLATE_ID)
                    .schemaId(UnicastRequestEncoder.SCHEMA_ID)
                    .version(UnicastRequestEncoder.SCHEMA_VERSION);

                return unicastClientDataHandler.call(requestBuffer, messageHeaderEncoder.size(), b);
            })
            .lift(operatorPublish).map(f -> null);
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
