package mes.domain.services;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Service;

import mes.domain.entity.User;
import mes.domain.repository.UserRepository;
import mes.domain.security.Pbkdf2Sha256;

 /*인터페이스 분리는 추후에 고민하자*/
@Service
public class AccountService {
	
	@Autowired
	UserRepository userRepository;	

	@Autowired
	SqlRunner sqlRunner;
		
	public User getUser(String username) {
		User user =null;
		if(username==null || username.length()==0 ) {
			return user;
		}			
		
		Optional<User> optUser = this.userRepository.findByUsername(username);
		
		if(optUser.isEmpty()==false) {
			user = optUser.get();
		}	
		
		return user;		
	}
	
	public boolean checkUserPassword(String plainText, User user) {
		
		String hashedPassword = user.getPassword();		
		boolean result = Pbkdf2Sha256.verification(plainText, hashedPassword);		
		return result;	
	}

	// 로그인&로그아웃시 login_log 테이블에 이력 저장
	public void saveLoginLog(String type, Authentication auth, HttpServletRequest request) {

		User user = (User) auth.getPrincipal();

		String clientIp = request.getHeader("X-Forwarded-For");
		if (clientIp == null || clientIp.isBlank()) {
			clientIp = request.getRemoteAddr();
		} else {
			clientIp = clientIp.split(",")[0].trim();
		}

        MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("type", type);
		paramMap.addValue("IPAddress", clientIp);
		paramMap.addValue("UserId", user.getId());
		paramMap.addValue("spjangcd", user.getSpjangcd());
		
		String sql = """
				insert into login_log("Type", "IPAddress", _created, "User_id", "spjangcd")
                VALUES (:type, :IPAddress ::inet, now(),:UserId, :spjangcd)
				""";
		
	    this.sqlRunner.execute(sql, paramMap);
	}


	 public List<Map<String, String>> findspjangcd() {

		 MapSqlParameterSource dicParam = new MapSqlParameterSource();

		 String sql = """
                SELECT spjangcd, spjangnm
                FROM tb_xa012
            """;
		 // SQL 실행
		 List<Map<String, Object>> rows = this.sqlRunner.getRows(sql, dicParam);

		 List<Map<String, String>> result = rows.stream()
				 .map(row -> {
					 Map<String, String> map = new HashMap<>();
					 map.put("spjangcd", (String) row.get("spjangcd"));
					 map.put("spjangnm", (String) row.get("spjangnm"));
					 return map;
				 })
				 .toList();

		 return result;
	 }

	 public List<Map<String, Object>> getBillPlans() {

		 MapSqlParameterSource dicParam = new MapSqlParameterSource();

		 String sql = """
                SELECT * FROM bill_plans
            """;
		 // SQL 실행
		 List<Map<String, Object>> results = this.sqlRunner.getRows(sql, dicParam);

		 return results;
	 }



}
