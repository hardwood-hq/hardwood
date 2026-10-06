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
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

final class BoundedBodySubscriber implements HttpResponse.BodySubscriber<byte[]> {

    private final byte[] bytes;
    private final CompletableFuture<byte[]> body = new CompletableFuture<>();
    private Flow.Subscription subscription;
    private int size;

    BoundedBodySubscriber(int limit) {
        bytes = new byte[limit];
    }

    @Override
    public CompletionStage<byte[]> getBody() {
        return body;
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
        if (this.subscription != null || body.isDone()) {
            subscription.cancel();
            return;
        }
        this.subscription = subscription;
        subscription.request(Long.MAX_VALUE);
    }

    @Override
    public void onNext(List<ByteBuffer> buffers) {
        if (body.isDone()) {
            return;
        }
        for (ByteBuffer buffer : buffers) {
            int length = buffer.remaining();
            if (length > bytes.length - size) {
                body.completeExceptionally(new BodyLimitException(bytes.length));
                subscription.cancel();
                return;
            }
            buffer.get(bytes, size, length);
            size += length;
        }
    }

    @Override
    public void onError(Throwable failure) {
        body.completeExceptionally(failure);
    }

    @Override
    public void onComplete() {
        if (!body.isDone()) {
            body.complete(Arrays.copyOf(bytes, size));
        }
    }

    static final class BodyLimitException extends IOException {

        private BodyLimitException(int limit) {
            super("S3 response body exceeds " + limit + " bytes");
        }
    }
}
