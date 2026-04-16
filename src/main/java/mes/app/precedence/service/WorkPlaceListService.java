package mes.app.precedence.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.transaction.Transactional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import mes.domain.services.SqlRunner;

@Service
public class WorkPlaceListService {

	@Autowired
	SqlRunner sqlRunner;
	
	public List<Map<String, Object>> getWorkPlaceList(String startDate, String endDate) {
		
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
				where b."TableName" = 'workplace_th_result'
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
				select b.id, b."Char1" as title, to_char(b."Date1", 'yyyy-MM-dd') as "DataDate"
                from bundle_head b
				inner join user_profile cu on b._creater_id = cu."User_id"
				left join user_profile uu on b._modifier_id = uu."User_id"
				left join v_appr_result r on b.id = r."SourceDataPk" and r."SourceTableName" = 'bundle_head'
				where b."TableName" = 'workplace_th_result'
                and b.id = :bhId
				""";
		
		Map<String,Object> mstInfo = this.sqlRunner.getRow(sql, paramMap);
		
		sql = """
				select mr.id, m."Code" as code ,m."Name" as name
						,fn_prop_data_number('master_t', m.id, 'temp_low' ) as temp_low
						,fn_prop_data_number('master_t', m.id, 'temp_upper' ) as temp_upper
						,fn_prop_data_number('master_t', m.id, 'humid_low' ) as humid_low
						,fn_prop_data_number('master_t', m.id, 'humid_upper' ) as humid_upper
						, mr."Char1" as result1, mr."Char2" as result2 , mr."Char3" as result3, mr."Char4" as result4, mr."Char5" as result5 
				from master_result mr
				inner join master_t m on m.id = mr."MasterTable_id" 
				where mr."SourceDataPk" = :bhId
				order by m."Code" 
			  """;
			
		List<Map<String,Object>> itemResult = this.sqlRunner.getRows(sql, paramMap);
		
		sql = """
                select da.id
		            , da."HappenDate" as happen_date, da."HappenPlace" as happen_place
		            , da."AbnormalDetail" as abnormal_detail, da."ActionDetail" as action_detail
		            , da."ActionState" as action_state, da."ConfirmState" as confirm_state
		            , da."SourceDataPk" as src_data_pk, da."SourceTableName" as src_table_name
		            , up."Name" as actor_name
		            , up2."Name" as creater_name
		            , RIGHT(da."AbnormalDetail", POSITION(':' in REVERSE(da."AbnormalDetail"))-1) as check_name
				    , ROW_NUMBER() OVER (ORDER BY mr.id) AS _order
	            from devi_action da
	            inner join master_result mr on mr.id = da."SourceDataPk" and da."SourceTableName" = 'workplace_th_result'
                and mr."SourceDataPk" = :bhId
	            left join user_profile up on up."User_id" = da._modifier_id
	            left join user_profile up2 on up2."User_id" = da._creater_id
                order by mr.id	
			  """;
		
		List<Map<String, Object>> itemDeviResult = this.sqlRunner.getRows(sql, paramMap);
		
		
		Map<String, Object> item = new HashMap<String, Object>();
		
		item.put("mst_info", mstInfo);
		item.put("item_result", itemResult);
		item.put("item_devi_result", itemDeviResult);
		
		return item;
	}

	@Transactional
	public void mstDelete(Integer bhId) {

		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("bhId", bhId);
		
		String sql = null;
		
		sql = """
            delete from master_result                     
            where "SourceDataPk" = :bhId
			""";
		
		this.sqlRunner.execute(sql, paramMap);
		
		sql = """
			    delete from devi_action
                where "SourceDataPk" not in (
	                select a.id
	                from master_result a
	                left join  devi_action b on b."SourceDataPk" = a.id
	                where 1=1
	                and "Char5" = 'X'
                )
                and "SourceTableName" = 'workplace_th_result'
			  """;
			
		this.sqlRunner.execute(sql, paramMap);
		
		sql = """
				delete from bundle_head where id = :bhId
			  """;
			
		this.sqlRunner.execute(sql, paramMap);
		
	}

}
