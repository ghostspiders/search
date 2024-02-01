
package org.example.action;

import org.example.http.RestStatus;
import org.jetbrains.annotations.Nullable;

import java.util.Objects;


public abstract class ShardOperationFailedException {

    protected String index;
    protected int shardId = -1;
    protected String reason;
    protected RestStatus status;
    protected Throwable cause;

    protected ShardOperationFailedException() {

    }

    protected ShardOperationFailedException(@Nullable String index, int shardId, String reason, RestStatus status, Throwable cause) {
        this.index = index;
        this.shardId = shardId;
        this.reason = Objects.requireNonNull(reason, "reason cannot be null");
        this.status = Objects.requireNonNull(status, "status cannot be null");
        this.cause = Objects.requireNonNull(cause, "cause cannot be null");
    }

    @Nullable
    public final String index() {
        return index;
    }


    public final int shardId() {
        return shardId;
    }


    public final String reason() {
        return reason;
    }


    public final RestStatus status() {
        return status;
    }


    public final Throwable getCause() {
        return cause;
    }
}
