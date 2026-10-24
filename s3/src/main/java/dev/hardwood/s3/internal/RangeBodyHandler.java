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
///
/// One handler serves every attempt of a range read. The direct buffer of a range outlives a
/// failed attempt with the bytes it received ([#received()]), and the next attempt, which
/// requests the bytes from [#resumeAt()] on, continues filling it from there. A `206` whose `ETag` is not the
/// `If-Match` the range was requested with is another version of the object: its body goes into
/// a heap buffer like an error document's, and the buffer of the range is left as it was.
public final class RangeBodyHandler implements HttpResponse.BodyHandler<RangeBody> {

    /// The most bytes received of a body other than a `206`'s.
    static final int OTHER_BODY_LIMIT = 64 * 1024;

    private static final int HTTP_OK = 200;
    private static final int HTTP_PARTIAL_CONTENT = 206;

    private final int length;
    private final boolean suffix;
    private final String ifMatch;
    /// The buffer of the range, allocated by the first `206` of the object version asked for.
    /// Attempts are sequential: each one's body completes before the next is sent.
    private ByteBuffer rangeTarget;

    /// @param length the number of bytes the range requests
    public RangeBodyHandler(int length) {
        this(length, false, null);
    }

    /// @param length  the number of bytes the range requests
    /// @param ifMatch the entity tag the range is requested with, or `null`
    public RangeBodyHandler(int length, String ifMatch) {
        this(length, false, ifMatch);
    }

    private RangeBodyHandler(int length, boolean suffix, String ifMatch) {
        this.length = length;
        this.suffix = suffix;
        this.ifMatch = ifMatch;
    }

    /// A handler for a suffix range of `length` bytes, whose body may be shorter, when the object
    /// is, and may come with a `200`.
    public static RangeBodyHandler suffix(int length) {
        return new RangeBodyHandler(length, true, null);
    }

    /// The bytes of the range received so far, by attempts that failed before their body ended.
    public int received() {
        return rangeTarget != null ? rangeTarget.position() : 0;
    }

    /// The position in the range the next attempt starts at. A range requested without `If-Match`
    /// starts over, since its resumed bytes could come from another version of the object. A range
    /// whose bytes have all arrived re-requests its last byte, as no range can start past its end.
    public int resumeAt() {
        return ifMatch != null ? Math.min(received(), length - 1) : 0;
    }

    @Override
    public HttpResponse.BodySubscriber<RangeBody> apply(HttpResponse.ResponseInfo responseInfo) {
        int status = responseInfo.statusCode();
        ByteBuffer target;
        if (suffix && (status == HTTP_PARTIAL_CONTENT || status == HTTP_OK)) {
            long declared = responseInfo.headers().firstValueAsLong("Content-Length").orElse(length);
            target = ByteBuffer.allocateDirect(Math.clamp(declared, 0, length));
        }
        else if (status == HTTP_PARTIAL_CONTENT && ofVersionAskedFor(responseInfo)) {
            if (rangeTarget == null) {
                rangeTarget = ByteBuffer.allocateDirect(length);
            }
            target = rangeTarget.position(resumeAt());
        }
        else {
            target = ByteBuffer.allocate(OTHER_BODY_LIMIT);
        }
        return new Receiver(target);
    }

    private boolean ofVersionAskedFor(HttpResponse.ResponseInfo responseInfo) {
        return ifMatch == null
                || responseInfo.headers().firstValue("ETag").map(ifMatch::equals).orElse(true);
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
