package iuh.fit.grpc.service;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

/** Tiện ích dùng chung cho các service gRPC: báo lỗi và bọc xử lý một RPC. */
final class Rpc {
    private static final Logger log = LoggerFactory.getLogger(Rpc.class);

    private Rpc() {}

    static void fail(StreamObserver<?> out, Status status) {
        out.onError(status.asRuntimeException());
    }

    /** Dùng để ném lỗi nghiệp vụ bên trong {@link #respond}: {@code throw Rpc.error(Status.NOT_FOUND, "...")}. */
    static StatusRuntimeException error(Status status, String message) {
        return status.withDescription(message).asRuntimeException();
    }

    /** Lỗi nghiệp vụ (StatusRuntimeException) giữ nguyên mã; lỗi khác trả INTERNAL và ghi log. */
    static <T> void respond(StreamObserver<T> out, Supplier<T> action, String failureMessage) {
        T result;
        try {
            result = action.get();
        } catch (StatusRuntimeException e) {
            out.onError(e);
            return;
        } catch (Exception e) {
            log.error(failureMessage, e);
            out.onError(Status.INTERNAL.withDescription(failureMessage).asRuntimeException());
            return;
        }
        out.onNext(result);
        out.onCompleted();
    }
}