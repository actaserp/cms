package mes;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import mes.domain.services.CommonUtil;
import mes.domain.services.SqlRunner;

@SpringBootTest
public class JDBCTypeTest {
	
	@Autowired(required = true)
    private NamedParameterJdbcTemplate  jdbcTemplate;

	@Autowired
	SqlRunner sqlRunner;
	
	@Test
	public void stringIntTest(){
		String sql = """
				select * from material where id = (:id)
				""";
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("id", "3");
		
		Map<String, Object> item = sqlRunner.getRow(sql, paramMap);
		System.out.println("item"+item.toString());
		

	}
	
	@Test
	public void stringIntTest2(){
		String sql = """
				select * from material where id = (:id)
				""";
		Map<String, Integer> dicParam = new HashMap<>();
		dicParam.put("id", 3);
		
    	MapSqlParameterSource namedParameters = new MapSqlParameterSource();
    	Map<String, Object> row = null;
    	
    	dicParam.forEach((key, val)->{
    		namedParameters.addValue(key, val);
    	});   	
    	
    	row = this.jdbcTemplate.queryForMap(sql, namedParameters);
    	System.out.println("item"+row.toString());
	}
	
	@Test
	public void timestampIntervalTest(){
		String sql = """
				select (:dateVal)::timestamp - interval '1 days'
				""";
		Map<String, String> dicParam = new HashMap<>();
		dicParam.put("dateVal", "2022-08-22");
		
    	MapSqlParameterSource namedParameters = new MapSqlParameterSource();
    	namedParameters.addValue("dateVal", "2022-08-22",java.sql.Types.TIMESTAMP);
    	Map<String, Object> row = null;
    	
    	row = this.jdbcTemplate.queryForMap(sql, namedParameters);
    	System.out.println("item"+row.toString());
	}
	
	@Test
	public void queryCommonutilCastTest(){
		String sql = """
				select id, "UnitPrice"
	            from mat_comp_uprice
	            limit 1
				""";
		
    	MapSqlParameterSource namedParameters = new MapSqlParameterSource();
    	Map<String, Object> row = null;
    	
    	row = this.jdbcTemplate.queryForMap(sql, namedParameters);
    	System.out.println("item : "+row.toString());
    	System.out.println("UnitPrice ori : "+row.get("UnitPrice"));
    	System.out.println("UnitPrice type : "+(row.get("UnitPrice")).getClass().getSimpleName());
    	System.out.println("UnitPrice float : "+CommonUtil.tryFloatNull(row.get("UnitPrice")));
    	System.out.println("UnitPrice double : "+CommonUtil.tryDoubleNull(row.get("UnitPrice")));
	}
}
