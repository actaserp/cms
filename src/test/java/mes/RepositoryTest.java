package mes;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import java.util.Optional;

import mes.domain.entity.Bom;
import mes.domain.entity.SystemLog;
import mes.domain.entity.SystemOption;
import mes.domain.entity.User;
import mes.domain.entity.UserGroup;
import mes.domain.entity.UserProfile;
import mes.domain.repository.BomRepository;
import mes.domain.repository.SystemLogRepository;
import mes.domain.repository.SystemOptionRepository;
import mes.domain.repository.UserRepository;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@SpringBootTest
public class RepositoryTest {

	@Autowired
	UserRepository userRepository;
	
	@Autowired
	SystemOptionRepository systemOptionRepository;
	
	@Autowired
	SystemLogRepository systemLogRepository;
	
	@Autowired
	BomRepository bomRepository;
		
	@Test
	public void bomTest() {
		
		Optional<Bom> optBom = this.bomRepository.findById(1);
		Bom bom = optBom.get();
		System.out.println(bom);
		
		Bom bom2 = this.bomRepository.getBomById(1);
		System.out.println(bom2);
		
	
	}
	
	@Test
    public void userTest() {    	
		Optional<User> optAdminUser = this.userRepository.findByUsername("admin");

			if(optAdminUser.isEmpty()==false) {
				User adminUser = optAdminUser.get();
				//System.out.println(adminUser);  
				Assert.assertTrue(adminUser.getUsername().equals("admin"));			
				UserProfile userProfile = adminUser.getUserProfile();			
				Assert.assertTrue(userProfile!=null);			
				UserGroup userGroup = userProfile.getUserGroup();			
				Assert.assertTrue(userGroup.getCode().equals("admin"));	
				
				String name = "관리자12";
				
				adminUser.getUserProfile().setName(name);			
				User u1 = this.userRepository.save(adminUser);			
				User u2 = this.userRepository.getUserById(u1.getId());
				
				Assert.assertTrue(name.equals(u2.getUserProfile().getName()));			
				
		}
    }
	
	@Test
	public void getByIdTest() throws JsonProcessingException {		
		Optional<User> optUser= this.userRepository.findById(2);
		User user = optUser.get();		
		Assert.assertTrue(user.getId()==2);		
		System.out.println(user.getUsername());		
		ObjectMapper mapper = new ObjectMapper();
		String json = mapper.writeValueAsString(user);		
		System.out.println(json);				
	}	
	
	@Test
	public void systemOptionRepositoryTest() {
		SystemOption sysOpt= this.systemOptionRepository.getByCode("LOGO_TITLE");
		System.out.println(sysOpt.getValue());
	}
	
	@Test
	public void systemLogRepositoryTest() {
		Optional<SystemLog>optSystemLog =  this.systemLogRepository.findById(1L);
		SystemLog systemLog = optSystemLog.get();
		System.out.println(systemLog.getMessage());
		
	}
}