package io.reactivex.aeron.unicast;

import java.io.Closeable;

/**
 * Created by rroeser on 6/5/15.
 */
public interface UnicastServer extends Closeable {
    void enableRateReport();
}
