/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.s3.internal;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.Flow;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class BoundedBodySubscriberTest {

    @Test
    void collectsValidPrefixesFromHeapDirectAndReadOnlyBuffers() {
        BoundedBodySubscriber subscriber = new BoundedBodySubscriber(5);
        Subscription subscription = new Subscription();
        subscriber.onSubscribe(subscription);
        ByteBuffer heap = ByteBuffer.wrap(new byte[] { 99, 1, 2, 99 });
        heap.position(1).limit(3);
        ByteBuffer direct = ByteBuffer.allocateDirect(1);
        direct.put((byte) 3).flip();
        subscriber.onNext(List.of(heap, direct, ByteBuffer.wrap(new byte[] { 4, 5 }).asReadOnlyBuffer()));
        assertThat(subscriber.getBody().toCompletableFuture().isDone()).isFalse();
        subscriber.onComplete();

        assertThat(subscriber.getBody().toCompletableFuture().join()).containsExactly(1, 2, 3, 4, 5);
        assertThat(subscription.cancelled).isFalse();
    }

    @Test
    void exceedingLimitCancelsSubscriptionAndDoesNotAcceptMoreData() {
        BoundedBodySubscriber subscriber = new BoundedBodySubscriber(2);
        Subscription subscription = new Subscription();
        subscriber.onSubscribe(subscription);
        subscriber.onNext(List.of(ByteBuffer.wrap(new byte[] { 1 }), ByteBuffer.wrap(new byte[] { 2, 3 })));
        subscriber.onNext(List.of(ByteBuffer.wrap(new byte[] { 4 })));
        subscriber.onComplete();

        assertThat(subscription.cancelled).isTrue();
        assertThatThrownBy(() -> subscriber.getBody().toCompletableFuture().join())
                .hasCauseInstanceOf(BoundedBodySubscriber.BodyLimitException.class);
    }

    @Test
    void preservesTransportFailure() {
        BoundedBodySubscriber subscriber = new BoundedBodySubscriber(2);
        subscriber.onSubscribe(new Subscription());
        IOException failure = new IOException("transport failure");
        subscriber.onError(failure);
        assertThatThrownBy(() -> subscriber.getBody().toCompletableFuture().join()).hasCause(failure);
    }

    @Test
    void acceptsEmptyBodyAndRejectsDuplicateSubscription() {
        BoundedBodySubscriber subscriber = new BoundedBodySubscriber(0);
        subscriber.onSubscribe(new Subscription());
        Subscription duplicate = new Subscription();
        subscriber.onSubscribe(duplicate);
        subscriber.onComplete();
        assertThat(duplicate.cancelled).isTrue();
        assertThat(subscriber.getBody().toCompletableFuture().join()).isEmpty();
    }

    private static final class Subscription implements Flow.Subscription {

        private boolean cancelled;

        @Override
        public void request(long count) {
        }

        @Override
        public void cancel() {
            cancelled = true;
        }
    }
}
