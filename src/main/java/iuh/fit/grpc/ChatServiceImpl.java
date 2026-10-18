package iuh.fit.grpc;

import iuh.fit.grpc.chat.ChatMessage;
import iuh.fit.grpc.chat.ChatResponse;
import iuh.fit.grpc.chat.ChatServiceGrpc;
import io.grpc.stub.StreamObserver;

public class ChatServiceImpl extends ChatServiceGrpc.ChatServiceImplBase {

    @Override
    public void sendMessage(ChatMessage request, StreamObserver<ChatResponse> responseObserver) {
        System.out.println("=== NHẬN REQUEST MỚI ===");
        System.out.println("Sender ID: " + request.getSenderId());
        System.out.println("Nội dung: " + request.getContent());

        ChatResponse response = ChatResponse.newBuilder()
                .setMessageId(request.getMessageId())
                .setIsSuccess(true)
                .setStatusMessage("Java Backend nhận tin nhắn thành công!")
                .setTimestamp(System.currentTimeMillis())
                .build();

        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}