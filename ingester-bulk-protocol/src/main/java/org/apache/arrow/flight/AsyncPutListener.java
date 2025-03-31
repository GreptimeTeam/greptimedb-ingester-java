/*
 * Copyright 2023 Greptime Team
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.arrow.flight;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.arrow.flight.grpc.StatusUtils;

/**
 * A handler for server-sent application metadata messages during a Flight DoPut operation.
 *
 * <p>To handle messages, create an instance of this class overriding {@link #onNext(PutResult)}. The other methods
 * should not be overridden.
 *
 * @see BulkFlightClient.PutListener
 */
public class AsyncPutListener implements BulkFlightClient.PutListener {

    private CompletableFuture<Void> completed;

    public AsyncPutListener() {
        this.completed = new CompletableFuture<>();
    }

    /**
     * Wait for the stream to finish on the server side. You must call this to be notified of any errors that may have
     * happened during the upload.
     */
    @Override
    public final void getResult() {
        try {
            this.completed.get();
        } catch (ExecutionException e) {
            throw StatusUtils.fromThrowable(e.getCause());
        } catch (InterruptedException e) {
            throw StatusUtils.fromThrowable(e);
        }
    }

    @Override
    public void onNext(PutResult val) {}

    @Override
    public final void onError(Throwable t) {
        this.completed.completeExceptionally(StatusUtils.fromThrowable(t));
    }

    @Override
    public final void onCompleted() {
        this.completed.complete(null);
    }

    @Override
    public boolean isCancelled() {
        return this.completed.isDone();
    }
}
