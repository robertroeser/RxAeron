package io.reactivex.aeron;

import rx.functions.Func3;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.MutableDirectBuffer;

/**
 * Created by rroeser on 6/10/15.
 */
public interface PublicationDataHandler<T> extends Func3<MutableDirectBuffer, Integer, T, DirectBuffer> {
    @Override
    DirectBuffer call(MutableDirectBuffer requestBuffer, Integer offset, T t);
}
