package mes;

import java.util.Optional;

import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;


import mes.domain.entity.User;
import mes.domain.repository.UserRepository;
import mes.domain.security.Pbkdf2Sha256;

@SpringBootTest
public class DjangoPasswordTest {
	
	@Autowired
	UserRepository userRepository;
	
	
	@Test
	public void password_Pbkdf2Sha256_Test() {
		
		Optional<User> optAdminUser = this.userRepository.findByUsername("admin");		
		if(optAdminUser.isEmpty()==false) {			
			User adminUser = optAdminUser.get();
			String password = adminUser.getPassword();
			System.out.println(password);
			//pbkdf2_sha256$180000$YVFKgvnG7bs5$xw2dyqw15VyWmF3p7WbZMCTGJVXmm/jIlWGlUnId4mU=			
			
			
			String testPassword = Pbkdf2Sha256.encode("yullin", "YVFKgvnG7bs5", 180000);
			System.out.println(testPassword);
			
			Assert.assertTrue(Pbkdf2Sha256.verification("yullin", password));
		}
	}
}
