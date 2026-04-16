package mes.app.support.service;

import java.util.List;
import java.util.Map;

import mes.app.common.TenantContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import mes.domain.services.SqlRunner;

@Service
public class ExcelFormService {

	@Autowired
	SqlRunner sqlRunner;
	
	// searchMainData
	public List<Map<String, Object>> getExcelFormList(String keyword) {
		String tenantId = TenantContext.get();
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("keyword", keyword);
		paramMap.addValue("spjangcd", tenantId);
		String sql = """
	           select id
                , "FormName" as form_name
                , to_char("StartDate", 'yyyy-mm-dd') || ' ~ ' || to_char("EndDate", 'yyyy-mm-dd') as apply_date
                , to_char(_created, 'yyyy-mm-dd') as created_date
                , "Description" as description
                from doc_form
                where "FormType" = 'excel'
                and spjangcd = :spjangcd
				""";
		
		if (!keyword.isEmpty() && keyword != null) {
        	sql += " and \"FormName\" ilike concat('%%',:keyword,'%%') ";
		}
		
		sql += " order by _created desc ";
		
		List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);
		
		return items;
	}
	
	// 엑셀양식 상세정보 조회
	public Map<String, Object> getExcelFormDetail(Integer id) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("form_id", id);
			
		String sql = """
	           select id
                , "FormName" as form_name
                , to_char("StartDate", 'yyyy-mm-dd') as start_date
                , to_char("EndDate", 'yyyy-mm-dd') as end_date
                , "Content" as form_value
                , "Description" as description
                from doc_form
                where id = :form_id
				""";
		
		Map<String, Object> item = this.sqlRunner.getRow(sql, paramMap);
		
		return item;
	}
}
