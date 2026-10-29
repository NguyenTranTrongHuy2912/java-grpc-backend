package iuh.fit.grpc;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.IOException;

public class GrpcServerApp {

    public static void main(String[] args) throws IOException, InterruptedException {
        int port = 50051;

        Server server = ServerBuilder.forPort(port)
                .addService(new ChatServiceImpl())
                .addService(new AuthServiceImpl())
                .build();

        System.out.println("Starting Java gRPC Server on port " + port + "...");
        server.start();
        System.out.println("Server started successfully!");

        server.awaitTermination();
    }
}