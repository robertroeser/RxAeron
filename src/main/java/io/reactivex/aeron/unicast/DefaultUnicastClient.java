package io.reactivex.aeron.unicast;

import io.reactivex.aeron.PublicationDataHandler;
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
    private final PublicationDataHandler<T> unicastClientDataHandler;
    private final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();

    public DefaultUnicastClient(Aeron aeron, String channel, int streamId,  PublicationDataHandler<T> unicastClientDataHandler) {
        this.channel = channel;
        this.streamId = streamId;
        this.unicastClientDataHandler = unicastClientDataHandler;
        this.publication = aeron.addPublication(channel, streamId);
        this.operatorPublish = new OperatorPublish(Schedulers.computation(), publication);

    }

    @Override
    public Observable<?> offer(Observable<T> buffer) {

        return buffer
            .map(b -> {
                UnsafeBuffer requestBuffer = new UnsafeBuffer(ByteBuffer.allocate(1024));

                messageHeaderEncoder.wrap(requestBuffer, 0, 0);

                messageHeaderEncoder
                    .blockLength(unicastClientDataHandler.getBlockLength())
                    .templateId(unicastClientDataHandler.getTemplateId())
                    .schemaId(unicastClientDataHandler.getSchemaId())
                    .version(unicastClientDataHandler.getSchemaVersion());

                return unicastClientDataHandler.call(requestBuffer, messageHeaderEncoder.size(), b);
            })
            .onBackpressureBlock()
            .lift(operatorPublish);
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
