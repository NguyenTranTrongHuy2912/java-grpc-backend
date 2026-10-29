package iuh.fit.grpc.mail;

public interface EmailSender {
    void sendOtp(String to, String code, int ttlMinutes) throws Exception;
}