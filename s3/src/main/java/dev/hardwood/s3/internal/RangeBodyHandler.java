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
/// other status, such as an error document, into a heap buffer of up to 64 KiB. A suffix range
/// ([#suffix(int)]) takes a `200` too, the answer of an endpoint that sends the whole object, and
/// either body goes into a direct buffer of its `Content-Length`, at most the suffix's length.
///
/// A body that fits is read to its end, so its HTTP/1.1 connection goes back to the pool. A body
/// longer than its buffer is cancelled once the buffer is full, which closes its HTTP/1.1
/// connection or resets its HTTP/2 stream; the result then reports the overflow. A body shorter
/// than the requested length completes with the bytes received, which the caller checks.
public final class RangeBodyHandler implements HttpResponse.BodyHandler<RangeBody> {

    /// The most bytes received of a body other than a `206`'s.
    static final int OTHER_BODY_LIMIT = 64 * 1024;

    private static final int HTTP_OK = 200;
    private static final int HTTP_PARTIAL_CONTENT = 206;

    private final int length;
    private final boolean suffix;

    /// @param length the number of bytes the range requests
    public RangeBodyHandler(int length) {
        this(length, false);
    }

    private RangeBodyHandler(int length, boolean suffix) {
        this.length = length;
        this.suffix = suffix;
    }

    /// A handler for a suffix range of `length` bytes, whose body may be shorter, when the object
    /// is, and may come with a `200`.
    public static RangeBodyHandler suffix(int length) {
        return new RangeBodyHandler(length, true);
    }

    @Override
    public HttpResponse.BodySubscriber<RangeBody> apply(HttpResponse.ResponseInfo responseInfo) {
        int status = responseInfo.statusCode();
        ByteBuffer target;
        if (suffix && (status == HTTP_PARTIAL_CONTENT || status == HTTP_OK)) {
            long declared = responseInfo.headers().firstValueAsLong("Content-Length").orElse(length);
            target = ByteBuffer.allocateDirect(Math.clamp(declared, 0, length));
        }
        else if (status == HTTP_PARTIAL_CONTENT) {
            target = ByteBuffer.allocateDirect(length);
        }
        else {
            target = ByteBuffer.allocate(OTHER_BODY_LIMIT);
        }
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
