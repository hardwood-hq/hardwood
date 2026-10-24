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
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Flow;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/// Drives the idle checks of an [IdleBoundSubscriber] by hand, through a timer that queues them.
class IdleBoundSubscriberTest {

    private final Deque<Runnable> checks = new ArrayDeque<>();
    private boolean cancelled;
    private IdleBoundSubscriber.Handler<byte[]> handler;
    private HttpResponse.BodySubscriber<byte[]> subscriber;

    @BeforeEach
    void setUp() {
        IdleTimer queued = (check, delay) -> checks.add(check);
        handler = new IdleBoundSubscriber.Handler<>(responseInfo -> HttpResponse.BodySubscribers.ofByteArray(),
                Duration.ofSeconds(30), queued, "GET s3://bucket/key range bytes=0-3");
        subscriber = handler.apply(null);
        subscriber.onSubscribe(new Flow.Subscription() {
            @Override
            public void request(long n) {
            }

            @Override
            public void cancel() {
                cancelled = true;
            }
        });
    }

    @Test
    void checkAfterBytesSchedulesNextCheck() throws Exception {
        subscriber.onNext(List.of(ByteBuffer.wrap(new byte[]{ 1, 2 })));
        checks.remove().run();
        subscriber.onNext(List.of(ByteBuffer.wrap(new byte[]{ 3, 4 })));
        checks.remove().run();
        subscriber.onComplete();
        checks.remove().run();

        assertThat(checks).isEmpty();
        assertThat(cancelled).isFalse();
        assertThat(handler.timeout()).isNull();
        assertThat(subscriber.getBody().toCompletableFuture().get()).containsExactly(1, 2, 3, 4);
    }

    @Test
    void checkWithoutBytesFailsBody() {
        subscriber.onNext(List.of(ByteBuffer.wrap(new byte[]{ 1, 2 })));
        checks.remove().run();
        checks.remove().run();
        // Signals after the timeout no longer reach the wrapped subscriber
        subscriber.onNext(List.of(ByteBuffer.wrap(new byte[]{ 3, 4 })));
        subscriber.onComplete();

        assertThat(checks).isEmpty();
        assertThat(cancelled).isTrue();
        CompletableFuture<byte[]> body = subscriber.getBody().toCompletableFuture();
        assertThatThrownBy(body::get)
                .isInstanceOf(ExecutionException.class)
                .cause()
                .isSameAs(handler.timeout())
                .isInstanceOf(IOException.class)
                .hasMessage("GET s3://bucket/key range bytes=0-3 timed out: no body bytes for 30000 ms");

        handler.reset();
        assertThat(handler.timeout()).isNull();
    }
}
