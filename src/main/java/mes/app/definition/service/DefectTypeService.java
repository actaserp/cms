package mes.app.definition.service;

import java.util.List;
import java.util.Map;

import mes.app.common.TenantContext;
import org.apache.groovy.parser.antlr4.util.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import mes.domain.services.SqlRunner;

@Service
public class DefectTypeService {
	@Autowired
	SqlRunner sqlRunner;
	
	// 부적합 유형 조회 
	public List<Map<String, Object>> getDefectTypeList(String keyword) {
		String tenantId = TenantContext.get();
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("keyword", keyword);
		dicParam.addValue("spjangcd", tenantId);

		String sql = """
			select dt.id
		    , dt."Code" as defect_type_code
		    , dt."Name" as defect_type_name
		    , dt."Description" as description
	        from defect_type dt
	        where 1=1
	        and dt.spjangcd = :spjangcd
			""";
		if (StringUtils.isEmpty(keyword)==false) sql+="and upper(dt.\"Name\") like concat('%%',upper(:keyword),'%%')";
		
		List<Map<String,Object>> items = this.sqlRunner.getRows(sql, dicParam);
		
		return items;
	}
	
	// 부적합 유형 상세 조회
	public Map<String, Object> getDefectTypeDetail(int id) {
		
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("id", id);
		
		String sql = """
			select dt.id
			, dt."Code" as defect_type_code
			, dt."Name" as defect_type_name
			, dt."Description" as description
			from defect_type dt
			where 1=1
			and dt.id = :id
			""";
		
		Map<String, Object> item = this.sqlRunner.getRow(sql, dicParam);
		
		return item;
	}
}
