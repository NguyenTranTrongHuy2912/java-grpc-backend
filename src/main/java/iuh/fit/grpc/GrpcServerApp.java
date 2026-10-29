package iuh.fit.grpc;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import iuh.fit.grpc.config.Env;
import iuh.fit.grpc.mail.ConsoleEmailSender;
import iuh.fit.grpc.mail.EmailSender;
import iuh.fit.grpc.mail.SmtpEmailSender;
import iuh.fit.grpc.service.AuthServiceImpl;
import iuh.fit.grpc.service.ChatServiceImpl;
import iuh.fit.grpc.service.OtpService;

import java.io.IOException;

public class GrpcServerApp {

    public static void main(String[] args) throws IOException, InterruptedException {
        int port = 50051;

        EmailSender emailSender = "smtp".equalsIgnoreCase(Env.get("MAIL_MODE", "console"))
                ? new SmtpEmailSender()
                : new ConsoleEmailSender();

        Server server = ServerBuilder.forPort(port)
                .addService(new ChatServiceImpl())
                .addService(new AuthServiceImpl(new OtpService(emailSender)))
                .build();

        System.out.println("Starting Java gRPC Server on port " + port + "...");
        server.start();
        System.out.println("Server started successfully!");

        server.awaitTermination();
    }
}