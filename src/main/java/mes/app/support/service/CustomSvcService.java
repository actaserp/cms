package mes.app.support.service;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import mes.domain.services.SqlRunner;

@Service
public class CustomSvcService {

	@Autowired
	SqlRunner sqlRunner;

	public List<Map<String, Object>> getCustomSvc(String startDt, String endDt, String combo) {

		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("startDt", startDt);
		paramMap.addValue("endDt", endDt);
		paramMap.addValue("combo", combo);
		
		String sql = """
				select cc.id, row_number() over() as no ,cc."Type" as type, cc."Title" as title,  cc."Qty" as "cusCnt"
				, cc."CheckDate" as "chkDt", cc."CheckName" as "chkName", cc."CheckState" as "chkState"
				, cc."FinishDate" as "finishDt", cc."Content" as "content"
				from cust_complain cc 
				where cc."CheckDate" between cast(:startDt as date) and cast(:endDt as date)
				""";
		
		if(StringUtils.hasText(combo)) {
			sql += " and cc.\"Type\"  = :combo ";
		}
		
		sql += " order by id ";
		
		List<Map<String,Object>> items = this.sqlRunner.getRows(sql, paramMap);
		
		return items;
	}

	public Map<String, Object> getCustomSvcDetail(Integer id) {

		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("Id", id);
		
		String sql = """
				select  cc.id
					  , cc."Type" as "cboType"
					  , cc."Title" as title
					  , cc."Qty" as "cusCnt"
					  , cc."CheckDate" as "chkDt"
					  , cc."CheckName" as "chkName"
					  , cc."CheckState" as "chkState"
					  , cc."FinishDate" as "finishDt"
					  , cc."Content" as "content"
					  from cust_complain cc 
					  where cc.id = :Id
				""";
		
		Map<String, Object> item = this.sqlRunner.getRow(sql, paramMap);
		return item;
	}
}
