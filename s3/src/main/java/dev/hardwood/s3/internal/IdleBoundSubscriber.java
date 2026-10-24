/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.s3.internal;

import java.io.IOException;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

/// Fails a response body that delivers no bytes for a whole idle period. The `HttpClient`'s
/// request timeout ends once the response headers arrive; this bounds the time between bytes of
/// the body that follows.
///
/// A check runs once per period, from the subscription on. It fails the body when no bytes
/// arrived since the previous check, so a stalled body fails after one to two periods without
/// bytes: it signals an [IOException] to the wrapped subscriber and cancels the subscription,
/// which closes the HTTP/1.1 connection or resets the HTTP/2 stream.
///
/// The client's signals reach the wrapped subscriber under the lock of a [Watch], and none
/// reaches it once a check has timed the body out, so a timeout cannot overlap a delivery of
/// bytes. A check still queued after the body ended holds only the watch, which has dropped the
/// wrapped subscriber and its buffer by then.
final class IdleBoundSubscriber<T> implements HttpResponse.BodySubscriber<T> {

    private final CompletionStage<T> body;
    private final Watch watch;

    private IdleBoundSubscriber(HttpResponse.BodySubscriber<T> delegate, Watch watch) {
        this.body = delegate.getBody();
        this.watch = watch;
    }

    @Override
    public CompletionStage<T> getBody() {
        return body;
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
        watch.subscribe(subscription);
    }

    @Override
    public void onNext(List<ByteBuffer> items) {
        watch.next(items);
    }

    @Override
    public void onError(Throwable throwable) {
        watch.error(throwable);
    }

    @Override
    public void onComplete() {
        watch.complete();
    }

    /// A body handler that wraps each subscriber it creates in an [IdleBoundSubscriber], and
    /// records the timeout of the body it timed out.
    static final class Handler<T> implements HttpResponse.BodyHandler<T> {

        private final HttpResponse.BodyHandler<T> handler;
        private final Duration period;
        private final IdleTimer timer;
        private final String description;
        private volatile IOException timeout;

        Handler(HttpResponse.BodyHandler<T> handler, Duration period, IdleTimer timer, String description) {
            this.handler = handler;
            this.period = period;
            this.timer = timer;
            this.description = description;
        }

        @Override
        public HttpResponse.BodySubscriber<T> apply(HttpResponse.ResponseInfo responseInfo) {
            HttpResponse.BodySubscriber<T> delegate = handler.apply(responseInfo);
            return new IdleBoundSubscriber<>(delegate, new Watch(delegate, this));
        }

        /// The failure of the body this handler's latest attempt timed out, or `null`. The client
        /// may fail the attempt with its own error for the cancelled connection instead, so the
        /// caller reports this one.
        IOException timeout() {
            return timeout;
        }

        /// Forgets the timeout of an earlier attempt, before the next one is sent.
        void reset() {
            timeout = null;
        }
    }

    /// The state of one body, shared by the subscriber and its queued checks.
    private static final class Watch {

        private final IdleTimer timer;
        private final Duration period;
        private Handler<?> handler;
        private HttpResponse.BodySubscriber<?> delegate;
        private Flow.Subscription subscription;
        private boolean done;
        private boolean progressed;

        Watch(HttpResponse.BodySubscriber<?> delegate, Handler<?> handler) {
            this.delegate = delegate;
            this.handler = handler;
            this.timer = handler.timer;
            this.period = handler.period;
        }

        synchronized void subscribe(Flow.Subscription subscription) {
            this.subscription = subscription;
            delegate.onSubscribe(subscription);
            timer.schedule(this::check, period);
        }

        synchronized void next(List<ByteBuffer> items) {
            if (!done) {
                progressed = true;
                delegate.onNext(items);
            }
        }

        synchronized void error(Throwable throwable) {
            if (!done) {
                end().onError(throwable);
            }
        }

        synchronized void complete() {
            if (!done) {
                end().onComplete();
            }
        }

        /// Marks the body done and drops the references a queued check would otherwise keep alive.
        private HttpResponse.BodySubscriber<?> end() {
            HttpResponse.BodySubscriber<?> ended = delegate;
            done = true;
            delegate = null;
            subscription = null;
            handler = null;
            return ended;
        }

        private void check() {
            HttpResponse.BodySubscriber<?> timedOut;
            Flow.Subscription cancelled;
            Handler<?> timing;
            synchronized (this) {
                if (done) {
                    return;
                }
                if (progressed) {
                    progressed = false;
                    timer.schedule(this::check, period);
                    return;
                }
                cancelled = subscription;
                timing = handler;
                timedOut = end();
            }
            IOException timeout = new IOException(timing.description + " timed out: no body bytes for "
                    + period.toMillis() + " ms");
            timing.timeout = timeout;
            // Outside the lock: no signal reaches the delegate once done is set, and a cancel that
            // signals synchronously cannot wait on a delivery that waits for the lock
            timedOut.onError(timeout);
            cancelled.cancel();
        }
    }
}
