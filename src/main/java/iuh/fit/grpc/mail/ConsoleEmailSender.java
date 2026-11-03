package iuh.fit.grpc.mail;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsoleEmailSender implements EmailSender {
    private static final Logger log = LoggerFactory.getLogger(ConsoleEmailSender.class);

    @Override
    public void sendOtp(String to, String code, int ttlMinutes, String purpose) {
        log.info(">>> [DEV] OTP ({}) gửi tới {}: {} (hết hạn sau {} phút)", purpose, to, code, ttlMinutes);
    }
}
