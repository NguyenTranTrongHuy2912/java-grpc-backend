package iuh.fit.grpc.mail;

import iuh.fit.grpc.config.Env;
import jakarta.mail.Authenticator;
import jakarta.mail.Message;
import jakarta.mail.PasswordAuthentication;
import jakarta.mail.Session;
import jakarta.mail.Transport;
import jakarta.mail.internet.InternetAddress;
import jakarta.mail.internet.MimeMessage;

import java.util.Properties;

public class SmtpEmailSender implements EmailSender {
    private final Session session;
    private final String from;

    public SmtpEmailSender() {
        String user = Env.require("SMTP_USER");
        String password = Env.require("SMTP_PASSWORD");
        this.from = Env.get("SMTP_FROM", user);

        Properties props = new Properties();
        props.put("mail.smtp.host", Env.get("SMTP_HOST", "smtp.gmail.com"));
        props.put("mail.smtp.port", Env.get("SMTP_PORT", "587"));
        props.put("mail.smtp.auth", "true");
        props.put("mail.smtp.starttls.enable", "true");
        props.put("mail.smtp.connectiontimeout", "5000");
        props.put("mail.smtp.timeout", "10000");
        props.put("mail.smtp.writetimeout", "10000");

        this.session = Session.getInstance(props, new Authenticator() {
            @Override
            protected PasswordAuthentication getPasswordAuthentication() {
                return new PasswordAuthentication(user, password);
            }
        });
    }

    @Override
    public void sendOtp(String to, String code, int ttlMinutes, String purpose) throws Exception {
        MimeMessage msg = new MimeMessage(session);
        msg.setFrom(new InternetAddress(from));
        msg.setRecipients(Message.RecipientType.TO, InternetAddress.parse(to, false));
        boolean reset = "reset_password".equals(purpose);
        String subject = reset ? "Mã đặt lại mật khẩu ChatApp" : "Mã xác thực đăng ký ChatApp";
        String intro = reset
                ? "Bạn vừa yêu cầu đặt lại mật khẩu. Mã xác thực của bạn là: "
                : "Mã xác thực đăng ký tài khoản của bạn là: ";
        String ignore = reset
                ? "Nếu bạn không yêu cầu đặt lại mật khẩu, hãy bỏ qua email này, mật khẩu của bạn vẫn an toàn."
                : "Nếu bạn không yêu cầu, hãy bỏ qua email này.";
        msg.setSubject(subject, "UTF-8");
        msg.setText(intro + code + "\n\n"
                + "Mã có hiệu lực trong " + ttlMinutes + " phút. "
                + ignore, "UTF-8");
        Transport.send(msg);
    }
}