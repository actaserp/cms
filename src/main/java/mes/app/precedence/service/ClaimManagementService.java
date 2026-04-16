package mes.app.precedence.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Service;

import mes.domain.entity.User;
import mes.domain.services.SqlRunner;

@Service
public class ClaimManagementService {
	
	@Autowired
	SqlRunner sqlRunner;

	public List<Map<String, Object>> getList(String start_date, String end_date) {
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("start_date", start_date);
		paramMap.addValue("end_date", end_date);
		
		
		
		String sql = """
				select b.id, b."Char1" as "Title", coalesce(r."StateName", '작성') as "StateName", r."LineName"
				 , r."LineNameState", to_char(b."Date1", 'yyyy-MM-DD') as "DataDate"
				 , coalesce(r."SearchYN", 'Y') as "SearchYN", coalesce(r."EditYN", 'Y') as "EditYN"
				 , coalesce(r."DeleteYN", 'Y') as "DeleteYN", b."Number1" as check_master_id
				 ,b._creater_id ,up."Name" as creater_name , b._modifier_id, up2."Name" as modifier_name
				 , cm."CheckCycle" , b."Text1"
				from bundle_head b                               
				left join v_appr_result r on b.id = r."SourceDataPk" and r."SourceTableName" = 'bundle_head'
				left join user_profile up on b._creater_id = up."User_id"
				left join user_profile up2 on b._modifier_id = up2."User_id"
				left join check_mast cm on cm.id = b."Number1"
				where b."TableName" = 'claim_management'
				and b."Date1" between cast(:start_date as date) and cast(:end_date as date)  
				""";
		
		
		List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);
		
		return items;
	}

	public Map<String, Object> apprList(Integer bhId, String data_date, Authentication auth) {
		User user = (User)auth.getPrincipal();
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("data_date", data_date);
		paramMap.addValue("bhId", bhId);	
		
		String sql = "";		
		
		Map<String, Object> head_info = new HashMap<>();
		
		Map<String, Object> diary_info = new HashMap<>();
		
		if(bhId > 0) {
			sql = """
					select bh.id
	                , bh."Char1" as "Title"
	                , bh."Char2" as "Description"
	                , to_char(bh."Date1", 'yyyy-MM-DD') as "DataDate"
	                , coalesce(uu."Name", cu."Name") as "FirstName"
	                , coalesce(r."State", 'write') as "State"
	                , coalesce(r."StateName", '작성') as "StateName"
	                from bundle_head bh
	                inner join user_profile cu on bh._creater_id = cu."User_id"
	                left join user_profile uu on bh._modifier_id = uu."User_id"
	                left join v_appr_result r on bh.id = r."SourceDataPk" and r."SourceTableName" = 'bundle_head'
	                where bh."TableName" = 'claim_management'
	                and bh.id = :bhId
					""";
			
			head_info = this.sqlRunner.getRow(sql, paramMap);
			
			sql ="""
					select 
					bh."Char1" as "Title", to_char(bh."Date2", 'yyyy-MM-DD') as pickupDate
					,bh."Text1", bh."Char2", bh."Char3", bh."Char4"
					from bundle_head bh
					where 1=1
					and bh.id = :bhId
					""";
			diary_info = this.sqlRunner.getRow(sql, paramMap);
			
		}else {
			head_info.put("id", 0);
			head_info.put("Title", "클레임관리일지");
			head_info.put("DataDate", data_date);
			head_info.put("FirstName", user.getUserProfile().getName());
			head_info.put("StateName", "상신대기");
			head_info.put("check_result_id", 0);
			head_info.put("Description", "");
			head_info.put("CheckStep", 1);
		}
		
		
		
		Map<String,Object> item = new HashMap<>();
		item.put("diary_info",diary_info);
		item.put("head_info",head_info);
		
		
		return item;
		
	}

	public void mstDelete(Integer bhId) {
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("bhId", bhId);
		String sql = "";
		
		sql = """
				delete from bundle_head 
				where 1=1
				and "TableName" = 'claim_management'
				and id = :bhId
				""";
		
		this.sqlRunner.execute(sql, paramMap);
	}

}
