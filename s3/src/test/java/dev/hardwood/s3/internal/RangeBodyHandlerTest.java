/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Copyright The original authors
 *
 *  Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package dev.hardwood.s3.internal;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Flow;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/// Checks [RangeBodyHandler] against a subscription whose `cancel()` signals `onError`
/// synchronously, as the JDK `HttpClient` does on an HTTP/2 stream.
class RangeBodyHandlerTest {

    @Test
    void overflowCompletesBeforeCancelSignalsError() {
        HttpResponse.BodySubscriber<RangeBody> subscriber = new RangeBodyHandler(4).apply(responseInfo(206));
        subscriber.onSubscribe(new Flow.Subscription() {
            @Override
            public void request(long n) {
            }

            @Override
            public void cancel() {
                subscriber.onError(new IOException("Stream 1 cancelled"));
            }
        });

        subscriber.onNext(List.of(ByteBuffer.wrap(new byte[]{ 1, 2, 3, 4, 5 })));

        RangeBody body = subscriber.getBody().toCompletableFuture().join();
        assertThat(body.overflow()).isTrue();
        assertThat(body.bytes().isDirect()).isTrue();
        assertThat(body.bytes().remaining()).isEqualTo(4);
        assertThat(body.bytes().get(3)).isEqualTo((byte) 4);
    }

    private static HttpResponse.ResponseInfo responseInfo(int status) {
        return new HttpResponse.ResponseInfo() {
            @Override
            public int statusCode() {
                return status;
            }

            @Override
            public HttpHeaders headers() {
                return HttpHeaders.of(Map.of(), (name, value) -> true);
            }

            @Override
            public HttpClient.Version version() {
                return HttpClient.Version.HTTP_2;
            }
        };
    }
}
