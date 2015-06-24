package io.reactivex.aeron.operators;

import io.reactivex.aeron.NotConnectedException;
import rx.Observable;
import rx.Scheduler;
import rx.Subscriber;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.agrona.DirectBuffer;

import java.util.concurrent.TimeUnit;

/**
 * Created by rroeser on 6/5/15.
 */
public class OperatorPublish implements Observable.Operator<Long, DirectBuffer> {
    private Scheduler scheduler;

    private Publication publication;

    public OperatorPublish(Scheduler scheduler, Publication publication) {
        this.scheduler = scheduler;
        this.publication = publication;
    }

    @Override
    public Subscriber<? super DirectBuffer> call(Subscriber<? super Long> child) {
        return new Subscriber<DirectBuffer>(child) {

            Scheduler.Worker worker = scheduler.createWorker();

            @Override
            public void onStart() {
                request(1);
                child.add(worker);
                worker = scheduler.createWorker();
            }

            @Override
            public void onCompleted() {
                child.onCompleted();
            }

            @Override
            public void onError(Throwable e) {
                child.onError(e);
            }

            @Override
            public void onNext(DirectBuffer directBuffer) {
                tryOffer(directBuffer);
            }

            public void tryOffer(DirectBuffer buffer) {
                long offer = publication.offer(buffer);

                if (offer >= 0) {
                    child.onNext(offer);
                    request(1);
                } else if (offer == Publication.NOT_CONNECTED) {
                    child.onError(new NotConnectedException());
                } else if (offer == Publication.BACK_PRESSURE) {
                    worker.schedule(() ->
                            tryOffer(buffer)
                    , 1, TimeUnit.MILLISECONDS);
                }

            }

        };
    }
}
