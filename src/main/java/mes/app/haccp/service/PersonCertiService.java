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
public class PersonCertiService {

	@Autowired
	SqlRunner sqlRunner;

	public List<Map<String, Object>> readPersonMedicalReport(String keyword, String deptId) throws JSONException {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("keyword", keyword);
		paramMap.addValue("deptId", deptId);
		
		String sql = """
				 with A as (
					    select pc."PersonName" as person_name
					    , max(pc."IssueDate") as issue_date
					    from person_certi pc 
					    where 1 = 1
					    and pc."CertificateCode" = 'person_medical_report'
					    group by pc."PersonName"
				    )
                    select pc.id, coalesce(d."Name",d2."Name") as dept_name, pc."PersonName" as person_name
                    , pc."TestDate" as test_date, pc."IssueDate" as issue_date
                    , pc."ExpireDate" as expire_date, pc."NextTestDate" as next_test_date
                    , pc."Description" as description
                    , (select array_to_json(ARRAY_AGG(case when af.id > 0 then json_build_object('file_id',af.id,'attach_name',af."AttachName",'file_name',af."FileName") else null end)) 
	                    from attach_file af 
					    where af."DataPk" = pc.id
	                    and af."TableName" = 'person_certi'
                    ) as files
	                from person_certi pc 
                    inner join A on A.person_name = pc."PersonName"
	                left join person p on p.id = pc."SourceDataPk"
	                and pc."SourceTableName" = 'person'
	                left join depart d on d.id = p."Depart_id"
	                left join user_profile u on u."User_id" = pc."SourceDataPk"
	                and pc."SourceTableName" = 'user_profile'
	                left join depart d2 on d2.id = u."Depart_id"
	                where 1 = 1
                    and pc."CertificateCode" = 'person_medical_report'
				""";
		
		if (keyword != null) {
			sql += " and upper(pc.\"PersonName\") like concat('%%',upper(:keyword),'%%') ";
		}
		
		if (deptId != null) {
			sql += " and cast(:deptId as Integer) in (d.id, d2.id) ";
		}
		
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);

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

	public Map<String, Object> personMedicalDetail(Integer id) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("id", id);
		
		String sql = """
				select pc.id, coalesce(d."Name",d2."Name") as dept_name, pc."PersonName"
                , pc."TestDate", pc."IssueDate", pc."ExpireDate", pc."NextTestDate"
                , pc."Description" as description
                from person_certi pc 
                left join person p on p.id = pc."SourceDataPk"
                and pc."SourceTableName" = 'person'
                left join depart d on d.id = p."Depart_id"
                left join user_profile u on u."User_id" = pc."SourceDataPk"
                and pc."SourceTableName" = 'user_profile'
                left join depart d2 on d2.id = u."Depart_id"
                where pc.id = :id
				""";
		
        Map<String, Object> item = this.sqlRunner.getRow(sql, paramMap);

        return item;
	}
}
