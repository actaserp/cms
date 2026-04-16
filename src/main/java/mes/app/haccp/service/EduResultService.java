package mes.app.haccp.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import mes.domain.entity.User;
import mes.domain.services.SqlRunner;

@Service
public class EduResultService {

	@Autowired
	SqlRunner sqlRunner;
	
	public List<Map<String, Object>> getEduResult(String dateFrom, String dateTo) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("dateFrom", dateFrom);
		paramMap.addValue("dateTo", dateTo);
		
		String sql = """
				select er.id, er."EduDate" as edu_date, er."EduTitle" as edu_title
                , er."EduPlace" as edu_place
	            , er."Teacher" as teacher
	            , er."EduHour" as edu_hour, er."StartTime" as start_time, er."EndTime" as end_time
	            , er."EduTarget" as edu_target
	            , er."TargetCount" as target_count, er."StudentCount" as student_count
	            , er."EduContent" as edu_content
	            , er."EduMaterial" as edu_material
	            , er."AbsenteeProcess" as absentee_process_code
	            , fn_code_name('edu_absentee_process', er."AbsenteeProcess") as absentee_process
	            , er."EduEvaluation"
	            from edu_result er
	            where er."EduDate" between cast(:dateFrom as date) and cast(:dateTo as date)
	            order by er."EduDate"
				""";

		List<Map<String,Object>> items = this.sqlRunner.getRows(sql, paramMap);
		
		return items;
	}

	public Map<String, Object> getEduResultDetail(Integer id) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("id", id);
		
		String sql = """
				select er.id, er."EduDate", er."EduTitle"
	            , er."EduPlace"
		        , er."Teacher"
		        , er."EduHour", er."StartTime", er."EndTime"
		        , er."EduTarget"
		        , er."TargetCount", er."StudentCount"
		        , er."EduContent"
		        , er."EduMaterial"
		        , er."AbsenteeProcess"
		        , er."EduEvaluation"
	            , er."SourceDataPk"
	            , er."SourceTableName"
		        from edu_result er
		        where er.id = :id
				""";
		
		Map<String, Object> item = this.sqlRunner.getRow(sql, paramMap);
		return item;
	}

	public Map<String, Object> apprStat(String startDate, String endDate, String apprState,
			String eduType) {

		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("apprState", apprState);
		paramMap.addValue("eduType", eduType);
		paramMap.addValue("startDate", startDate);
		paramMap.addValue("endDate", endDate);
		
		String sql = """
				select b.id, b."Char1" as "Title", coalesce(r."StateName", '작성') as "StateName", r."LineName", r."LineNameState", b."Char2" as "EduType"
                , to_char(b."Date1", 'yyyy-MM-dd') as "DataDate", coalesce(r."SearchYN", 'Y') as "SearchYN", coalesce(r."EditYN", 'Y') as "EditYN"
                , coalesce(r."DeleteYN", 'Y') as "DeleteYN"                
                from bundle_head b
                left join v_appr_result r on b.id = r."SourceDataPk" and r."SourceTableName" = 'bundle_head'
                where b."TableName" = 'edu_result_diary'
                and b."Date1" between cast(:startDate as date) and cast(:endDate as date)
				""";
		if (!eduType.isEmpty() && eduType != null) {
			sql += " and b.\"Char2\" = :eduType ";
		}
		
		if (!apprState.isEmpty() && apprState != null) {
			if (apprState.equals("write")) {
				sql += " and r.\"State\" is null ";
			} else {
				sql += " and r.\"State\" = :apprState ";
			}
		}
        
        sql += " order by b.\"Date1\" desc  ";
        
		List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);
		
		Map<String, Object> map = new HashMap<>();
		map.put("document_info", items);
		
		return map;
	}

	public Map<String, Object> getEduResult(Integer bhId, String dataDate, String eduType, User user) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("bhId", bhId);
		paramMap.addValue("dataDate", dataDate);
		paramMap.addValue("eduType", eduType);
		
		String sql = null;
		
		Map<String, Object> headInfo = new HashMap<>();
		
		List<Map<String,Object>> studentInfo = this.sqlRunner.getRows(sql, paramMap);
		if (bhId > 0) {
			sql = """
					select b.id, b."Char1" as "Title", coalesce(r."StateName", '작성') as "StateName", r."LineName", r."LineNameState", coalesce(uu."Name", cu."Name") as "FirstName"
                    , b."Char2" as "EduType", to_char(b."Date1", 'yyyy-MM-dd') as "DataDate", coalesce(r."SearchYN", 'Y') as "SearchYN", coalesce(r."EditYN", 'Y') as "EditYN"
                    , coalesce(r."DeleteYN", 'Y') as "DeleteYN"                
                    from bundle_head b
					inner join user_profile cu on b._creater_id = cu."User_id"
					left join user_profile uu on b._modifier_id = uu."User_id"
					left join v_appr_result r on b.id = r."SourceDataPk" and r."SourceTableName" = 'bundle_head'
					where b."TableName" = 'edu_result_diary'
                    and b.id = :bhId
					""";
			
			headInfo = this.sqlRunner.getRow(sql, paramMap);
			
			sql = """
					select ers."StudentName" , ers.id from edu_result er 
					inner join edu_result_student ers on ers."EduResult_id"  = er.id 
					where er."SourceDataPk"  = :bhId
				  """;
			
			studentInfo = this.sqlRunner.getRows(sql, paramMap);
			
			
		} else {
			headInfo.put("id", 0);
			headInfo.put("Title", "교육결과보고서 일지");
			headInfo.put("DataDate", dataDate);
			headInfo.put("FirstName", user.getUserProfile().getName());
			headInfo.put("EduType", eduType);
			headInfo.put("State", "write");
			headInfo.put("StateName", "작성");
		}
		
		sql = """
			select er.id, er."EduDate" as edu_date, er."EduTitle" as edu_title
            , er."EduPlace" as edu_place
	        , er."Teacher" as teacher
	        , er."EduHour" as edu_hour, er."StartTime" as start_time, er."EndTime" as end_time
	        , er."EduTarget" as edu_target
	        , er."TargetCount" as target_count, er."StudentCount" as student_count
	        , er."EduContent" as edu_content
	        , er."EduMaterial" as edu_material
	        , er."AbsenteeProcess" as absentee_process_code
	        , fn_code_name('edu_absentee_process', er."AbsenteeProcess") as absentee_process
	        , er."EduEvaluation" as edu_evaluation
	        from edu_result er
	        where er."SourceDataPk" = :bhId
	        order by er."EduDate"
			""";
		
		List<Map<String,Object>> diaryInfo = this.sqlRunner.getRows(sql, paramMap);
		
		
		Map<String, Object> items = new HashMap<>();
		
		items.put("head_info", headInfo);
		items.put("diary_info", diaryInfo);
		items.put("student_info", studentInfo);
		
		return items;
	}
}
