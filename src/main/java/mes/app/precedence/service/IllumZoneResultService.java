package mes.app.precedence.service;

import java.sql.Date;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import io.micrometer.core.instrument.util.StringUtils;
import mes.domain.entity.User;
import mes.domain.services.SqlRunner;

@Service
public class IllumZoneResultService {

	@Autowired
	SqlRunner sqlRunner;
	
	public Map<String, Object> getIllumResultList(int bh_id, String search_year
			,String search_month,String type2,User user){
	
		String search_date = search_year + "-" + String.format("%02d", Integer.parseInt(search_month));
		String base_date = search_year + "-" + String.format("%02d", Integer.parseInt(search_month)) + "-01";
		
//		String date_from = base_date;
		
//		LocalDate initial = LocalDate.of(Integer.parseInt(search_year), Integer.parseInt(search_month), 01);
//		LocalDate date_to = initial.withDayOfMonth(initial.lengthOfMonth());
		
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		Map<String,Object> head_info = new HashMap<String,Object>();
		List<Map<String, Object>> diary_info = new ArrayList<Map<String, Object>>();
		List<Map<String, Object>> action_info = new ArrayList<Map<String, Object>>();
		
		Map<String,Object> items = new HashMap<String,Object>();
		
		if(bh_id>0) {
			//일지 헤더
			dicParam.addValue("bh_id", bh_id);
			
			String sql = """
	        	select b.id, b."Char1" as "Title", b."Char2" as "Year", b."Char3" as "Month", b."Text1" as "Floor", coalesce(uu."Name", cu."Name") as "FirstName", coalesce(r."State", 'write') as "State", coalesce(r."StateName", '작성') as "StateName"
                from bundle_head b
				inner join user_profile cu on b._creater_id = cu."User_id"
				left join user_profile uu on b._modifier_id = uu."User_id"
				left join v_appr_result r on b.id = r."SourceDataPk" and r."SourceTableName" = 'bundle_head'
				where b."TableName" = 'illum_zone'
                and b.id = :bh_id
        		""";
				    
			head_info = this.sqlRunner.getRow(sql, dicParam);
            
			dicParam.addValue("base_date", base_date);
			dicParam.addValue("search_date", search_date);
			dicParam.addValue("type2", head_info.get("Floor"));
			
            sql = """
	        	with S as (
				    select t."Type" as code, t."Number1" as standard
				    from master_t t
				    where t."MasterClass" = 'illum_zone_standard'
				), A as (
				    select t.id as master_id, t."Name" as master_name, t."Char1" as area_num
				        , t."Type" as type_code
				        , fn_user_code_name('illum_zone_class', t."Type") as type_name
				        , t."Type2" as type2_code
				        , fn_user_code_name('층', t."Type2") as type2_name
				        , t._order
				    from master_t t
				    where t."MasterClass" = 'illum_zone'
				    and t."Type2" = :type2
				), B as (
				    select A.master_id, r.id
				        , row_number() over (partition by A.master_id order by r."DataDate" desc, r."DataTime" desc) as g_idx
				        , r."DataDate", left(cast(r."DataTime" as text),5) as "DataTime", r."Number1"
				    from master_result r
				    inner join A on A.master_id = r."MasterTable_id"
				    where to_char(r."DataDate",'YYYY-MM') like concat('%%', :search_date ,'%%')
				    and r."SourceDataPk" = :bh_id
				)
				select A._order, A.master_id, A.master_name, A.type_code, A.type_name, A.area_num
				    , A.type2_code, A.type2_name
				    , S.standard
				    , B."DataDate" as data_date
				    , B."Number1" as result
				    , B.id
				    , da."AbnormalDetail" as abnormal_detail
				    , da."ActionDetail" as action_detail
				    , case 
				        when (S.standard > B."Number1") then 'X'
				        when (B."Number1" is null) then ''
				        else 'O' 
				    end as res_judgment
				from A 
				left join B on B.master_id = A.master_id
				and B.g_idx = 1 
				left join S on S.code = A.type_code
				left join devi_action da on da."SourceDataPk" = B.id
				and da."SourceTableName" = 'illum_zone'
				order by A._order, A.type_code desc
        		""";
            
            diary_info = this.sqlRunner.getRows(sql, dicParam);
            
            sql = """
    	        select da.id, mr.id as master_result_id, m._order
				    , da."HappenDate" as happen_date, m."Name" as happen_place
				    , da."AbnormalDetail" as abnormal_detail, da."ActionDetail" as action_detail
				    , da."ActionState" as action_state, da."ConfirmState" as confirm_state
				    , da."SourceDataPk" as src_data_pk, da."SourceTableName" as src_table_name
				    , up."Name" as actor_name
				    , up2."Name" as creater_name
				from devi_action da
				inner join master_result mr on mr.id = da."SourceDataPk" and da."SourceTableName" = 'illum_zone'
				inner join master_t m on m.id = mr."MasterTable_id"
				and mr."SourceDataPk" = :bh_id
				left join user_profile up on up."User_id" = da._modifier_id
				left join user_profile up2 on up2."User_id" = da._creater_id
				order by mr.id
            		""";
            
            action_info = this.sqlRunner.getRows(sql, dicParam);
            
		} else {
			head_info.put("id", 0);
			head_info.put("Title", "영업장 조도점검일지("+type2+")");
			head_info.put("Year", search_year);
			head_info.put("Month", search_month);
			head_info.put("Floor", type2);
			head_info.put("FirstName", user.getUserProfile().getName());
			head_info.put("State", "write");
			head_info.put("StateName", "작성");
			
			dicParam = new MapSqlParameterSource();
			dicParam.addValue("base_date", base_date);
//			dicParam.addValue("date_from", date_from);
//			dicParam.addValue("date_to", date_to);
			dicParam.addValue("type2", type2);
						
			String sql = """
		        	with S as (	
	                    select t."Type" as code, t."Number1" as standard
	                    from master_t t
	                    where t."MasterClass" = 'illum_zone_standard'
	                    and cast(:base_date as date) between t."StartDate" and t."EndDate"
                    ), A as (
	                    select t.id as master_id, t."Name" as master_name, t."Char1" as area_num
                        , t."Type" as type_code
                        , fn_user_code_name('illum_zone_class', t."Type") as type_name
                        , t."Type2" as type2_code
                        , fn_user_code_name('층', t."Type2") as type2_name
                        , t._order
	                    from master_t t
	                    where t."MasterClass" = 'illum_zone'
	                    and to_char(cast(:base_date as date),'YYYY-MM') between to_char(t."StartDate",'YYYY-MM') and to_char(t."EndDate",'YYYY-MM')
                        and t."Type2" = :type2
                    )
                    select A._order, A.master_id, A.master_name, A.type_code, A.type_name , A.area_num
                    , A.type2_code, A.type2_name
                    , S.standard
                    , null as data_date
                    , null as result
                    , null as id
                    , null as abnormal_detail
                    , null as action_detail
                    , null as res_judgment
                    from A 
                    left join S on S.code = A.type_code
                    order by A._order, A.type_code desc
	        		""";
			
			diary_info = this.sqlRunner.getRows(sql, dicParam);
			
//			action_info = null;
		}
		items.put("head_info", head_info);
        items.put("diary_info", diary_info);
        items.put("action_info", action_info);

        return items;
	}

	// 관리구역) 관리구역 리스트 조회
	public List<Map<String, Object>> getMasterList(String master_class, String base_date) {

		MapSqlParameterSource dicParam = new MapSqlParameterSource();        
        dicParam.addValue("master_class", master_class);      
        dicParam.addValue("base_date", Date.valueOf(base_date));
        
        String sql = """
        		with S as (	
	                select t."Type" as code, t."Number1" as standard
	                from master_t t
	                where t."MasterClass" = 'illum_zone_standard'
	                and :base_date between t."StartDate" and t."EndDate"
	            ), A as (
	                select t.id as master_id, t."Name" as master_name
	                , t."Type" as type_code
	                , fn_user_code_name('illum_zone_class', t."Type") as type_name
	                from master_t t
	                where t."MasterClass" = :master_class
	                and :base_date between t."StartDate" and t."EndDate"
	            ), B as (
	                select A.master_id, r.id 
	                , row_number() over (partition by A.master_id order by r."DataDate" desc, r."DataTime" desc) as g_idx
	                , r."DataDate", r."DataTime", r."Number1"
	                from master_result r
	                inner join A on A.master_id = r."MasterTable_id"
	            )
	            select A.master_id, A.master_name, A.type_code, A.type_name
	            , S.standard
	            , concat(B."DataDate", ' ', B."DataTime") as last_date
	            , B."Number1" as last_illum
	            ,B.id
	            from A 
	            left join B on b.master_id = A.master_id
	            and B.g_idx = 1 
	            left join S on S.code = A.type_code
	            where 1 = 1
        		""";
  			    
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);

        return items;
	}
	
	// 결과입력탭) 상세정보 조회 detail_master
	public Map<String, Object> getMasterDetailList(Integer master_id, Integer id){
		
		MapSqlParameterSource dicParam = new MapSqlParameterSource();  
        dicParam.addValue("master_id", master_id);        
        dicParam.addValue("id", id);      
        
        String sql = """
        		select t.id as master_id
	            , t."Name" as master_name
	            , r.id, r."DataDate", r."DataTime", r."Number1"
	            , r."Description"
	            from master_t t
	            left join master_result r on r."MasterTable_id" = t.id 
	             and r.id = :id
	             where t.id = :master_id
		    """;
        
        Map<String, Object> item = this.sqlRunner.getRow(sql, dicParam);

        return item;
	}
		
	// 측정결과탭) 관리구역 리스트. 최종 조도값과 최종측정일을 표시
	public List<Map<String, Object>> getMasterResultList(Integer master_id, String data_year) {

		MapSqlParameterSource dicParam = new MapSqlParameterSource();        
        dicParam.addValue("master_id", master_id);      
        dicParam.addValue("start_date", Date.valueOf(data_year + "-01-01"));
        dicParam.addValue("end_date", Date.valueOf(data_year + "-12-31"));
        
        String sql = """
        		select r.id
        		, t.id as master_id
        		, t."Name" as master_name
	            ,r."DataDate" as data_date
	            , r."DataTime" as data_time
	            , r."Number1" as number1
	            , r."Description" as description
	            from master_t t
	            inner join master_result r on r."MasterTable_id" = t.id 
	            where r."MasterTable_id" = :master_id
        		""";
        
        if (StringUtils.isEmpty(data_year) == false) {
        	sql += "  and r.\"DataDate\" between :start_date and :end_date ";
        }
        
        sql += " order by r.\"DataDate\", r.\"DataTime\" ";
  			    
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);

        return items;
	}
	
}
