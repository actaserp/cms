package mes.app.definition.service;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import mes.domain.services.SqlRunner;

@Service
public class DepartService {

	@Autowired
	SqlRunner sqlRunner;
	
	public List<Map<String, Object>> getDepart(String keyword, String spjangcd) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("keyword", keyword);
		paramMap.addValue("spjangcd", spjangcd);
		
		String sql = """
				SELECT d.id
		        , d."Name" as dept_name
		        , d."Code" as dept_code
		        , d."Description" as description
		        , d."Type" as dept_type
		        FROM depart d
		        where 1=1
		        AND d.spjangcd = :spjangcd
				""";
		if(StringUtils.hasText(keyword)) {
			sql += """
					and ( d."Name" like concat('%%',:keyword,'%%')
				     	or d."Code" like concat('%%',:keyword,'%%')
				     	)
					""";
		}
		
        sql += " order by d.\"Name\" ";
        
		List<Map<String,Object>> items = this.sqlRunner.getRows(sql, paramMap);
		
		return items;
	}

	public Map<String, Object> getDepartDetail(int id) {

		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("id", id);

		String sql = """
				SELECT d.id
		        , d."Name" 
		        , d."Code"
		        , d."Description"
	            , d."Type"
		        FROM depart d
	            where d.id = :id
				""";
		
		
		Map<String, Object> item = this.sqlRunner.getRow(sql, paramMap);
		
		return item;
	}

}
