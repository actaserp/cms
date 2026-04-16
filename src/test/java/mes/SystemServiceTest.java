package mes;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.util.StringUtils;

import mes.app.system.service.SystemService;
import mes.domain.entity.User;
import mes.domain.entity.UserGroup;
import mes.domain.entity.UserProfile;
import mes.domain.repository.UserRepository;

@SpringBootTest
public class SystemServiceTest {

	@Autowired
	SystemService systemService;
		
	@Autowired
	UserRepository userRepository;
	
	@Autowired
    private NamedParameterJdbcTemplate  jdbcTemplate;
	
	
	@Test
	public void webmenuTest() {
		
		Optional<User> optAdminUser = this.userRepository.findByUsername("admin");		
		if(optAdminUser.isEmpty()==false) {
			User adminUser = optAdminUser.get();
			//System.out.println(adminUser);  
			Assert.assertTrue(adminUser.getUsername().equals("admin"));			
			UserProfile userProfile = adminUser.getUserProfile();			
			Assert.assertTrue(userProfile!=null);			
			UserGroup userGroup = userProfile.getUserGroup();			
			Assert.assertTrue(userGroup.getCode().equals("admin"));
			
			List<Map<String, Object>> items = this.systemService.getWebMenuList(adminUser);
			
			System.out.println(items);			
		}
	}
	
	@Test
	public void systemLogList() {
		
		String start = "2022-06-01 00:00:00";
		String end = "2022-08-10 23:59:59";
		String type="";
		String source = "";
 
		//Timestamp tsStart = Timestamp.valueOf(start);
		//Timestamp tsEnd = Timestamp.valueOf(end);
		
    	String sql = """    			
                select id
                , "Type" as type
                , "Source" as source
                ,"Message" as message
                , to_char("_created" ,'yyyy-mm-dd hh24:mi:ss') as created
                from sys_log sl
                where _created between :start and :end    			
        	""";
        	
        	if(StringUtils.hasText(type)) {
        		sql +="""    				
        		and "Type" ilike concat('%',:type,'%')
        		""";
        	}
        	
        	if(StringUtils.hasText(source)) {
        		sql +="""
        		and "Source" ilike concat('%', :source, '%')		
        		""";
        	}
        	sql+="""
        	order by _created desc		
        """;
        	
    	
    	MapSqlParameterSource namedParameters = new MapSqlParameterSource();
    	
    	namedParameters.addValue("start", start, java.sql.Types.TIMESTAMP);
    	namedParameters.addValue("end", end, java.sql.Types.TIMESTAMP);
    	namedParameters.addValue("type", type);
    	namedParameters.addValue("source", source);
        	
        List<Map<String, Object>> items = this.jdbcTemplate.queryForList(sql, namedParameters);
        System.out.println(items);	
		
		
	}
	
}
