package io.reactivex.aeron.requestreply;

import java.io.Closeable;

/**
 * Created by rroeser on 6/8/15.
 */
public interface RequestReplyServer extends Closeable {
    void enableRateReport();

}
