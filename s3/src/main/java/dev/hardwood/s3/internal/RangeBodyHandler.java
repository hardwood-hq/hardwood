/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.s3.internal;

import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

/// Receives the body of a range `GET` into a buffer as the client delivers it, on the client's
/// threads. A `206` body goes into a direct buffer of the requested length, the body of any
/// other status, such as an error document, into a heap buffer of up to 64 KiB.
///
/// A body that fits is read to its end, so its HTTP/1.1 connection goes back to the pool. A body
/// longer than its buffer is cancelled once the buffer is full, which closes its HTTP/1.1
/// connection or resets its HTTP/2 stream; the result then reports the overflow. A body shorter
/// than the requested length completes with the bytes received, which the caller checks.
public final class RangeBodyHandler implements HttpResponse.BodyHandler<RangeBody> {

    /// The most bytes received of a body other than a `206`'s.
    static final int OTHER_BODY_LIMIT = 64 * 1024;

    private static final int HTTP_PARTIAL_CONTENT = 206;

    private final int length;

    /// @param length the number of bytes the range requests
    public RangeBodyHandler(int length) {
        this.length = length;
    }

    @Override
    public HttpResponse.BodySubscriber<RangeBody> apply(HttpResponse.ResponseInfo responseInfo) {
        ByteBuffer target = responseInfo.statusCode() == HTTP_PARTIAL_CONTENT
                ? ByteBuffer.allocateDirect(length)
                : ByteBuffer.allocate(OTHER_BODY_LIMIT);
        return new Receiver(target);
    }

    private static final class Receiver implements HttpResponse.BodySubscriber<RangeBody> {

        private final ByteBuffer target;
        private final CompletableFuture<RangeBody> body = new CompletableFuture<>();
        private Flow.Subscription subscription;

        Receiver(ByteBuffer target) {
            this.target = target;
        }

        @Override
        public CompletionStage<RangeBody> getBody() {
            return body;
        }

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            this.subscription = subscription;
            subscription.request(Long.MAX_VALUE);
        }

        @Override
        public void onNext(List<ByteBuffer> items) {
            // Items already in flight when the subscription was cancelled still arrive
            if (body.isDone()) {
                return;
            }
            for (ByteBuffer item : items) {
                if (item.remaining() > target.remaining()) {
                    target.put(item.slice(item.position(), target.remaining()));
                    // Completed before the cancel, which on HTTP/2 signals onError synchronously
                    body.complete(new RangeBody(target.flip(), true));
                    subscription.cancel();
                    return;
                }
                target.put(item);
            }
        }

        @Override
        public void onError(Throwable throwable) {
            body.completeExceptionally(throwable);
        }

        @Override
        public void onComplete() {
            if (!body.isDone()) {
                body.complete(new RangeBody(target.flip(), false));
            }
        }
    }
}
