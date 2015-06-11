package io.reactivex.aeron;

import rx.Observable;
import rx.functions.Func3;
import uk.co.real_logic.agrona.DirectBuffer;

/**
 * Created by rroeser on 6/10/15.
 */
public interface SubscriptionDataHandler extends Func3<DirectBuffer, Integer, Integer, Observable<Void>> {
    @Override
    Observable<Void> call(DirectBuffer buffer, Integer offset, Integer length);
}
