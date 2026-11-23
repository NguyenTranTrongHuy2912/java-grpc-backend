package iuh.fit.grpc;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import iuh.fit.grpc.config.Env;
import iuh.fit.grpc.mail.ConsoleEmailSender;
import iuh.fit.grpc.mail.EmailSender;
import iuh.fit.grpc.mail.HttpEmailSender;
import iuh.fit.grpc.mail.SmtpEmailSender;
import iuh.fit.grpc.service.AuthServiceImpl;
import iuh.fit.grpc.service.ChatServiceImpl;
import iuh.fit.grpc.service.OtpService;

import java.io.IOException;

public class GrpcServerApp {

    public static void main(String[] args) throws IOException, InterruptedException {
        int port = Integer.parseInt(Env.get("PORT", "50051"));

        // console: in OTP ra log | smtp: gửi qua SMTP | brevo: gửi qua HTTP API (dùng khi host chặn cổng SMTP)
        String mailMode = Env.get("MAIL_MODE", "console");
        EmailSender emailSender = switch (mailMode.toLowerCase()) {
            case "smtp" -> new SmtpEmailSender();
            case "brevo" -> new HttpEmailSender();
            default -> new ConsoleEmailSender();
        };

        Server server = ServerBuilder.forPort(port)
                .addService(new ChatServiceImpl())
                .addService(new AuthServiceImpl(new OtpService(emailSender)))
                .build();

        System.out.println("Starting Java gRPC Server on port " + port + " (MAIL_MODE=" + mailMode + ")...");
        server.start();
        System.out.println("Server started successfully!");

        server.awaitTermination();
    }
}