package mes.app.weather.service;

import mes.domain.services.SqlRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class TB_xusersService {

  @Autowired
  SqlRunner sqlRunner;


  // 로그인 한 사람의 사업장 주소 가지고 감
  public String getUserAddress(String userId) {
    MapSqlParameterSource params = new MapSqlParameterSource();
    params.addValue("userid", userId);
    // 사용자 주소를 조회하는 SQL 쿼리 작성
    String sql = """
            SELECT adresa AS address
            FROM tb_xa012 
            WHERE spjangcd = (select spjangcd from auth_user where username = :userid)
            """;

    List<Map<String, Object>> result = sqlRunner.getRows(sql, params);

    // 결과가 존재하면 address 반환, 없으면 null 반환
    if (result != null && !result.isEmpty() && result.get(0).get("address") != null) {
      return result.get(0).get("address").toString();
    }
    return null;
  }
}
