package io.reactivex.aeron;

import io.reactivex.aeron.protocol.MessageHeaderEncoder;
import io.reactivex.aeron.protocol.UnicastRequestEncoder;
import io.reactivex.aeron.unicast.UnicastClient;
import io.reactivex.aeron.unicast.UnicastServer;
import org.junit.Test;
import rx.Observable;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.nio.ByteBuffer;

/**
 * Created by rroeser on 6/6/15.
 */
public class RxAeronRateReportTest {
    public static final String CHANNEL = "aeron:udp?remote=localhost:43450";

    //@Test
    public void testRateWithRXAeron() throws Exception {
        RxAeron instance = RxAeron.getInstance();

        UnicastClient rateTestClient = instance.createUnicastClient(CHANNEL);
        UnicastServer rateTestServer = instance.createUnicastServer(CHANNEL, ob -> ob.map(b -> null));
        rateTestServer.enableRateReport();

        Observable<DirectBuffer> buffer = Observable
            .range(1, 1_000_000)
            .map(i -> "sending_" + i)
            .map(s -> new UnsafeBuffer(s.getBytes()));

        rateTestClient
            .offer(buffer)
            .toBlocking().last();

        rateTestClient.close();
        rateTestServer.close();
    }

    @Test
    public void testSendWithoutOperator() throws Exception {
        RxAeron rxAeron = RxAeron.getInstance();
        UnicastServer unicastServer = rxAeron.createUnicastServer(CHANNEL, ob -> ob.map(b -> null));
        unicastServer.enableRateReport();

        Publication publication = rxAeron.aeron.addPublication(CHANNEL, 1);


        UnsafeBuffer requestBuffer = new UnsafeBuffer(ByteBuffer.allocateDirect(1024));

        MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
        UnicastRequestEncoder unicastRequestEncoder = new UnicastRequestEncoder();

        for (int i = 1; i < 1_000_000; i++) {

            messageHeaderEncoder.wrap(requestBuffer, 0, 0);

            messageHeaderEncoder
                .blockLength(UnicastRequestEncoder.BLOCK_LENGTH)
                .templateId(UnicastRequestEncoder.TEMPLATE_ID)
                .schemaId(UnicastRequestEncoder.SCHEMA_ID)
                .version(UnicastRequestEncoder.SCHEMA_VERSION);

            unicastRequestEncoder.wrap(requestBuffer, messageHeaderEncoder.size());

            byte[] payload = ("sending_" + i).getBytes();
            unicastRequestEncoder.putPayload(payload, 0, payload.length);

            while (publication.offer(requestBuffer) < 0) {
                // TODO: backoff?
            }

        }

        publication.close();
        unicastServer.close();


    }



}
