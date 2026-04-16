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
public class HandOverService {

	@Autowired
	SqlRunner sqlRunner;
	
	// 결재현황 조회
	public List<Map<String, Object>> getHandOverApprStatus(String startDate, String endDate, String apprState) {

		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("startDate", startDate);
		paramMap.addValue("endDate", endDate);
		paramMap.addValue("apprState", apprState);
        
		String sql = """
				select b.id, b."Char1" as "Title", coalesce(r."State", 'write') as "State", coalesce(r."StateName", '상신대기') as "StateName", r."LineName", r."LineNameState"
				, to_char(b."Date1", 'yyyy-MM-dd') as "DataDate", coalesce(r."SearchYN", 'Y') as "SearchYN", coalesce(r."EditYN", 'Y') as "EditYN", coalesce(r."DeleteYN", 'Y') as "DeleteYN"
				from bundle_head b
				left join v_appr_result r on b.id = r."SourceDataPk" and r."SourceTableName" = 'bundle_head'
				where b."TableName" = 'hand_over'
				and b."Date1" between cast(:startDate as date) and cast(:endDate as date)
				""";
		
		if (!apprState.isEmpty() && apprState != null) {
            if (apprState.equals("write")) {
            	sql += " and r.\"State\" is null ";
            } else {
            	sql += " and r.\"State\" = :apprState ";
            }
		}
		
		sql += " order by b.\"Date1\" desc ";
  			    
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);
        
        return items;
	}
	
	public Map<String, Object> getHandOverIn(Integer bh_id, String data_date, User user) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("bh_id", bh_id);
		
		Map<String,Object> head_info = new HashMap<String,Object>();
		Map<String,Object> diary_info = new HashMap<String,Object>();
		Map<String,Object> items = new HashMap<String,Object>();
		
		if(bh_id > 0) {
			String sql = """
				select b.id, b."Char1" as "Title", coalesce(r."State", 'write') as "State", coalesce(r."StateName", '상신대기') as "StateName", r."LineName", r."LineNameState", coalesce(uu."Name", cu."Name") as "FirstName"
                , to_char(b."Date1", 'yyyy-MM-dd') as "DataDate", coalesce(r."SearchYN", 'Y') as "SearchYN", coalesce(r."EditYN", 'Y') as "EditYN", coalesce(r."DeleteYN", 'Y') as "DeleteYN"               
                from bundle_head b
				inner join user_profile cu on b._creater_id = cu."User_id"
				left join user_profile uu on b._modifier_id = uu."User_id"
				left join v_appr_result r on b.id = r."SourceDataPk" and r."SourceTableName" = 'bundle_head'
				where b."TableName" = 'hand_over'
                and b.id = :bh_id
				""";
				
			head_info = this.sqlRunner.getRow(sql, paramMap);
		}else {
			head_info.put("id", 0);
			head_info.put("Title", "인수인계 일지");
			head_info.put("DataDate", data_date);
			head_info.put("FirstName", user.getUserProfile().getName());
			head_info.put("State", "write");
			head_info.put("StateName", "상신대기");
		}
		
		String sql = """
				select h.id, h."StartDate" as start_date, h."EndDate" as end_date, h."Reason" as reason
	            , h."FromName" as from_name, h."FromTel" as from_tel
	            , h."ToName" as to_name, h."ToTel" as to_tel, h."Description" as description
	            , h."SourceDataPk" as bh_id
	            from hand_over h
	            where 1 = 1
		        and h."SourceDataPk" = :bh_id
		        order by h."StartDate"
				""";
				
		diary_info = this.sqlRunner.getRow(sql, paramMap);
		
		items.put("head_info", head_info);
        items.put("diary_info", diary_info);
		
		return items;
	}

	public List<Map<String, Object>> getHandOver(String dataYear) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("dataYear", dataYear);
		
		String sql = """
				select h.id, h."StartDate" as start_date, h."EndDate" as end_date, h."Reason" as reason
	            , h."FromName" as from_name, h."FromTel" as from_tel
		        , h."ToName" as to_name, h."ToTel" as to_tel, h."Reason" as reason, h."Description" as content
		        from hand_over h
		        where 1 = 1
				""";
		
		if(!dataYear.isEmpty()) {
			sql += """
					and h."StartDate" <= cast(concat(:dataYear,'-12-31') as date)
			        and h."EndDate" >= cast(concat(:dataYear,'-01-01') as date)
					""";
		}
		sql += " order by h.\"StartDate\" ";
				
		List<Map<String,Object>> items = this.sqlRunner.getRows(sql, paramMap);
		
		return items;
	}


}
