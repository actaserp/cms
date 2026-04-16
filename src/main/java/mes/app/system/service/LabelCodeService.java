package mes.app.system.service;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import io.micrometer.core.instrument.util.StringUtils;
import mes.domain.services.SqlRunner;

@Service
public class LabelCodeService {

	@Autowired
	SqlRunner sqlRunner;

	public List<Map<String, Object>> getLabelCodeList(String menu_code) {
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("moduleName", menu_code);
		
		String sql = """ 
				  select lc.id
	             , lc."ModuleName"
	             , coalesce ((select "MenuName" from menu_item where "MenuCode"=lc."ModuleName" limit 1), 'Common') as menu_name
	             , lc."TemplateKey"
	             , lc."LabelCode"
	             , lc."Description"
	             , to_char("_created" ,'yyyy-mm-dd hh24:mi:ss') as created
	            from label_code lc 
	            where lc."ModuleName"= :moduleName
	            """;
	    
		List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);
		
		return items;	
	}

	public Map<String, Object> getLabelCodeDetail(int id) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("id", id);

		String sql = """
			 select lc.id
             , lc."ModuleName"
             , coalesce ((select "MenuName" from menu_item where "MenuCode"=lc."ModuleName" limit 1), 'Common') as menu_name
             , (select "MenuFolder_id" from menu_item where "MenuCode"=lc."ModuleName" limit 1) as "MenuFolder_id"
             , lc."TemplateKey"
             , lc."LabelCode"
             , lc."Description"
             , to_char("_created" ,'yyyy-mm-dd hh24:mi:ss') as created
            from label_code lc 
            where lc.id = :id
	        """;
			
			Map<String, Object> item = this.sqlRunner.getRow(sql, paramMap);
			return item;
	}

	public List<Map<String, Object>> getLabelLanguageList(Integer labelcode_id, String lang_code) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("labelcode_id", labelcode_id);
		paramMap.addValue("lang_code", lang_code);
		
		String sql = """
				 select lcl.id
                , lc."ModuleName"
                , lc."TemplateKey"
                , lcl."LangCode" 
                , lc."LabelCode" 
                , lcl."DispText" 
                , lcl."LabelCode_id"
                , to_char(lcl."_created" ,'yyyy-mm-dd hh24:mi:ss') as created
                , fn_code_name('lang_code', lcl."LangCode") as lang_code_name
                from label_code_lang lcl 
                inner join label_code lc on lcl."LabelCode_id" = lc.id
                where lcl."LabelCode_id" = :labelcode_id
				""";
		if (StringUtils.isEmpty(lang_code)==false) sql +="and \"LangCode\" = :lang_code";		
		
		List<Map<String,Object>> items = this.sqlRunner.getRows(sql, paramMap);
		return items;
	}

	public Map<String, Object> getLabellangDetail(int id) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("id", id);
		
		String sql = """
				select lcl.id
                , lc."ModuleName"
                , lc."TemplateKey"
                , lcl."LangCode" 
                , lc."LabelCode" 
                , lcl."DispText" 
                , lcl."LabelCode_id"
                , to_char(lcl."_created" ,'yyyy-mm-dd hh24:mi:ss') as created
                from label_code_lang lcl 
                inner join label_code lc on lcl."LabelCode_id" = lc.id
                where lcl.id = :id
	        """;
		
		Map<String, Object> item = this.sqlRunner.getRow(sql, paramMap);
		return item;
	}

	public int getChkCode(Integer labelCodeId, String langCode) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("labelCodeId", labelCodeId);
		paramMap.addValue("langCode", langCode);
		
		String sql = """
				select *
				from label_code_lang lcl
				where "LabelCode_id" = :labelCodeId and "LangCode" = :langCode
	        """;
		List<Map<String,Object>> item = this.sqlRunner.getRows(sql, paramMap);
		return item.size();
	}
	
}
