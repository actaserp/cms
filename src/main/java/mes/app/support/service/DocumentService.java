package mes.app.support.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import mes.app.common.TenantContext;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;


import io.micrometer.core.instrument.util.StringUtils;
import mes.domain.services.SqlRunner;

@Service
public class DocumentService {

	@Autowired
	SqlRunner sqlRunner;
	// 문서 목록조회
	public List<Map<String, Object>> getDocumentList(String formId, String keyword) throws JSONException {
		String tenantId = TenantContext.get();
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("form_id", formId);
		paramMap.addValue("keyword", keyword);
		paramMap.addValue("spjangcd", tenantId);

		String sql = """
                    select dr.id as id
                    , f."FormName" as form_name
                    , dr."DocumentName" as doc_name
                    , dr."Content" as content
                    , to_char(dr."DocumentDate",'yyyy-mm-dd') as doc_date
                    , (select array_to_json(ARRAY_AGG(case when af.id > 0 then json_build_object('file_id',af.id,'attach_name',af."AttachName",'file_name',af."FileName") else null end)) 
	                        from attach_file af 
						    where af."DataPk" = dr.id
                            and af."TableName" = 'doc_result'
                            ) as files                    
	                from doc_result dr 
	                inner join doc_form f on f.id = dr."DocumentForm_id"
	                where f."FormType" = 'file'
	                and f.spjangcd = :spjangcd
            """;
		
		if (StringUtils.isEmpty(keyword)==false) sql +="and upper(dr.\"DocumentName\") like concat('%%', upper(:keyword), '%%')";
		if (StringUtils.isEmpty(formId)==false) sql+="and dr.\"DocumentForm_id\" = cast(:form_id as integer) ";
		
		sql += "order by 1, 2";
		
		List<Map<String,Object>> items = this.sqlRunner.getRows(sql, paramMap);
		
		for(int i=0; i < items.size(); i++) {
			if (items.get(i).get("files") != null) {
				
				JSONArray jr = new JSONArray(items.get(i).get("files").toString());
				
				List<Map<String,Object>> list = new ArrayList<Map<String,Object>>();
				
				for (int j = 0; j < jr.length(); j ++) {
					JSONObject jo = (JSONObject) jr.get(j);
					
					Map<String,Object> map = new HashMap<String,Object>();
					map.put("file_id", jo.get("file_id"));
					map.put("attach_name", jo.get("attach_name"));
					map.put("file_name", jo.get("file_name"));
					
					list.add(map);
				}
				items.get(i).replace("files", list);
			}
		}
		return items;
	}
	// 문서 상세조회
	public Map<String, Object> getDocumentDetail(int id) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("id", id);
		
		String sql = """
            select dr.id as id
            , f."FormName" as form_name
            , f.id as doc_form_id
            , dr."DocumentName" as doc_name
            , dr."Content" as content
            , to_char(dr."DocumentDate",'yyyy-mm-dd') as doc_date
	        from doc_result dr 
	        inner join doc_form f on f.id = dr."DocumentForm_id"
		    where 1=1 
            and dr.id= :id
        """;
		
		Map<String, Object> item = this.sqlRunner.getRow(sql, paramMap);
		return item;
	}

}
