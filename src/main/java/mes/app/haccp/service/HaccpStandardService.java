package mes.app.haccp.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import mes.domain.services.SqlRunner;

@Service
public class HaccpStandardService {

	@Autowired
	SqlRunner sqlRunner;
	
	// 관리기준서 조회
	public List<Map<String, Object>> getDocumentList(Integer form_id, String last) throws JSONException {
				
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
                    where 1 = 1
	                and f."FormGroup" = '관리기준서'
                    and f."FormType" = 'file'
        		""";
  			    
	    if ("Y".equals(last)) {
	    	
	    	sql += """
	    			and dr."DocumentDate" = (select max(dr2."DocumentDate") 
                                            from doc_result dr2 
                                            where dr2."DocumentForm_id" = dr."DocumentForm_id")
	    			""";
	    }
	    
        sql += " order by dr.\"DocumentDate\" desc  ";
       
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, null);

		for (int i = 0; i < items.size(); i++) {
			
			if (items.get(i).get("files") != null) {
				
				JSONArray jr = new JSONArray(items.get(i).get("files").toString());
				
				List<Map<String,Object>> list = new ArrayList<Map<String,Object>>();
				
				for (int j=0; j < jr.length(); j ++) {
					
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

	// 관리기준서 상세조회
	public Map<String, Object> getDocumentDetailList(Integer id){
		
		MapSqlParameterSource dicParam = new MapSqlParameterSource();        
        dicParam.addValue("id", id);
        
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
        
        Map<String, Object> item = this.sqlRunner.getRow(sql, dicParam);
        
        return item;
	}

	public List<Map<String, Object>> getStandardRead(String startDate, String endDate) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("startDate", startDate);
		paramMap.addValue("endDate", endDate);
		
		String sql = """
				select b.id, b."Char1" as "Title", coalesce(r."StateName", '작성') as "StateName"
				, r."LineName", r."LineNameState", to_char(b."Date1", 'yyyy-MM-dd') as "DataDate"
				, coalesce(r."SearchYN", 'Y') as "SearchYN", coalesce(r."EditYN", 'Y') as "EditYN"
				, coalesce(r."DeleteYN", 'Y') as "DeleteYN", b."Number1" as check_master_id
				, b._creater_id ,up."Name" as "creater_name" , b._modifier_id, up2."Name" as "modifier_name"
				from bundle_head b                               
				left join v_appr_result r on b.id = r."SourceDataPk" and r."SourceTableName" = 'bundle_head'
				left join user_profile up on b._creater_id = up."User_id"
				left join user_profile up2 on b._modifier_id = up2."User_id"
				where b."TableName" = 'haccp_standard_doc'
				and b."Date1" between cast(:startDate as date) and cast(:endDate as date)
				""";
		
		sql += " order by b.\"Date1\" desc ";
		
		List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);
		
		return items;
	}

	public Map<String, Object> getResultList(Integer bhId) {

		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("bhId", bhId);
		
		String sql = null;
		
		sql = """
				select b.id, b."Char1" as title, to_char(b."Date1", 'yyyy-MM-dd') as "DataDate", coalesce(uu."Name", cu."Name") as "createName"
                from bundle_head b
				inner join user_profile cu on b._creater_id = cu."User_id"
				left join user_profile uu on b._modifier_id = uu."User_id"
				left join v_appr_result r on b.id = r."SourceDataPk" and r."SourceTableName" = 'bundle_head'
				where b."TableName" = 'haccp_standard_doc'
                and b.id = :bhId
				""";
		
		Map<String,Object> mstInfo = this.sqlRunner.getRow(sql, paramMap);
		
		sql = """
				select dr.id as doc_id 
					 , dr."DocumentForm_id" as "cboDocForm"
					 , dr."Content" as content
					 , dr."DocumentDate" as doc_date
					 , dr."DocumentName" as doc_name
				from doc_result dr 
				where dr."Text1" = 'bundle_head'
				and dr."Number1" = :bhId
			  """;
		
		Map<String,Object> detailInfo = this.sqlRunner.getRow(sql, paramMap);
		
		Map<String, Object> item = new HashMap<String, Object>();
		
		item.put("mst_info", mstInfo);
		item.put("detail_info", detailInfo);
		
		return item;
	}
}
