package mes.app;


import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.FileSystemResource;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.JavaMailSenderImpl;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.stereotype.Service;

import javax.mail.MessagingException;
import javax.mail.internet.MimeMessage;
import java.io.File;
import java.util.List;

@Slf4j
@Service
public class MailService {


    @Autowired
    private JavaMailSender mailSender;

    @Value("${spring.mail.username}")
    private String fromEmail;

    public void sendVerificationEmail(String to, String usernm, String uuid, String content){
        String subject = content + " 인증 메일입니다.";
        String text = "안녕하세요, " + usernm + "님.\n\n"
                + "다음 인증 코드를 입력하여 " + content + "을 완료하세요:\n"
                + uuid + "\n\n"
                + "이 코드는 3분 동안 유효합니다.";

        SimpleMailMessage message = new SimpleMailMessage();

        message.setTo(to);
        message.setSubject(subject);
        message.setText(text);
        message.setFrom(fromEmail);

        mailSender.send(message);
    }

    public void sendMailWithAttachment(List<String> recipients, String subject, String body, File attachment, String attachmentFileName) {
        try {
            MimeMessage message = mailSender.createMimeMessage();
            MimeMessageHelper helper = new MimeMessageHelper(message, true, "UTF-8");

            helper.setTo(recipients.toArray(new String[0]));
            helper.setSubject(subject);
            helper.setText(body, true);
            // 이렇게 맞춰야 됨 (메일 전송 계정과 동일)
            helper.setFrom(fromEmail);

            FileSystemResource file = new FileSystemResource(attachment);
            helper.addAttachment(attachmentFileName, file);

            mailSender.send(message);

            JavaMailSenderImpl impl = (JavaMailSenderImpl) mailSender;
            /*log.info("📨 실제 연결 시도 host: {}", impl.getHost());
            log.info("📨 실제 연결 시도 port: {}", impl.getPort());
            log.info("✅ 메일 전송 성공");
            log.info("📧 SMTP HOST : {}", impl.getHost());
            log.info("📧 SMTP PORT : {}", impl.getPort());
            log.info("📧 USERNAME   : {}", impl.getUsername());*/

        } catch (MessagingException e) {
            log.error("❌ 메일 전송 실패", e);
            throw new RuntimeException("메일 전송 실패", e);
        }
    }

}
