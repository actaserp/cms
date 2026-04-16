package mes.config;

import mes.domain.repository.UserRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.JavaMailSenderImpl;

import java.util.Properties;

@Configuration
public class MailConfig {

    @Autowired
    UserRepository userRepository;

    @Value("${spring.mail.username}")
    private String fromEmail;

    @Value("${spring.mail.password}")
    private String mailPassword;

    private JavaMailSenderImpl mailSender;

    @Bean
    public JavaMailSender getJavaMailSender() {
        JavaMailSenderImpl mailSender = new JavaMailSenderImpl();

        mailSender.setHost("smtp.naver.com");
        mailSender.setPort(465);
        mailSender.setUsername(fromEmail);
        mailSender.setPassword(mailPassword); // 반드시 앱 비밀번호

        Properties props = mailSender.getJavaMailProperties();
        props.put("mail.transport.protocol", "smtp");
        props.put("mail.smtp.auth", "true");
        props.put("mail.smtp.ssl.enable", "true");
        props.put("mail.smtp.starttls.enable", "false");
       // props.put("mail.debug", "true");

        return mailSender;
    }



    /*@Bean
    public JavaMailSender getJavaMailSender2(){


        if(mailSender == null){
            mailSender = new JavaMailSenderImpl();
//            updateMailSender();
        }

        return mailSender;

    }
*/
    /*// SMTP 설정을 동적으로 업데이트하는 메서드
    public void updateMailSender() {
        Optional<User> byUsername = userRepository.findByUsername("admin");

        String smtpid = "";
        String smtppw = "";

        if (byUsername.isPresent()) {
            smtpid = byUsername.get().getSmtpid();
            smtppw = byUsername.get().getSmtppassword();
        }

        mailSender.setHost("smtp.naver.com");
        mailSender.setUsername(smtpid);
        mailSender.setPassword(smtppw);
        mailSender.setPort(587);

        Properties props = mailSender.getJavaMailProperties();
        props.put("mail.transport.protocol", "smtp");
        props.put("mail.smtp.auth", "true");
        props.put("mail.smtp.starttls.enable", "true");
        props.put("mail.smtp.starttls.required", "true");
        props.put("mail.debug", "true");
        props.put("mail.smtp.ssl.trust", "smtp.naver.com");
        props.put("mail.smtp.ssl.protocols", "TLSv1.2");
    }*/
}
