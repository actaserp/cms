package mes.app.precedence.service;

import java.sql.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import mes.domain.services.SqlRunner;

@Service
public class PlaceOutCheckListService {

	@Autowired
	SqlRunner sqlRunner;

	// 결재현황 조회
	public List<Map<String, Object>> getPlaceOutCheckListApprStat(String start_date, String end_date, String appr_state) {

		MapSqlParameterSource dicParam = new MapSqlParameterSource();        
        dicParam.addValue("start_date", Date.valueOf(start_date));   
        dicParam.addValue("end_date", Date.valueOf(end_date));
        dicParam.addValue("appr_state", appr_state);
        
        String sql = """
        		select b.id, b."Char1" as "Title"
        		, coalesce(r."State", 'write') as "State"
        		, coalesce(r."StateName", '작성') as "StateName"
        		, r."LineName", r."LineNameState"
                , to_char(b."Date1", 'YYYY-MM-DD') as "DataDate"
                , coalesce(r."SearchYN", 'Y') as "SearchYN"
                , coalesce(r."EditYN", 'Y') as "EditYN"
                , coalesce(r."DeleteYN", 'Y') as "DeleteYN"
                from bundle_head b
                left join v_appr_result r on b.id = r."SourceDataPk" and r."SourceTableName" = 'bundle_head'
                where b."TableName" = 'place_out_check_list'
                and b."Date1" between :start_date and :end_date
        		""";
        
        if (!appr_state.isEmpty() && appr_state != null) {
            if (appr_state.equals("write")) {
            	sql += " and r.\"State\" is null ";
            } else {
            	sql += " and r.\"State\" = :appr_state ";
            }
		}
		
        sql += " order by b.\"Date1\" desc, b.id desc ";
  			    
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);
        
        return items;
	}
	
	// 점검표 조회
	public Map<String, Object> getPlaceOutCheckList(Integer bh_id) {
		
	    MapSqlParameterSource paramMap = new MapSqlParameterSource();
	    paramMap.addValue("bh_id", bh_id);
        
        Map<String, Object> items = new HashMap<>();
        
	    if (bh_id > 0) {
	        String sql = """
	        		select b.id, b."Char1" as "Title", to_char(b."Date1", 'yyyy-MM-dd') as "DataDate",
					    coalesce(uu."Name", cu."Name") as "FirstName",
					    coalesce(r."State", 'write') as "State",
					    coalesce(r."StateName", '작성') as "StateName"
					from "bundle_head" b
					inner join "user_profile" cu on b._creater_id = cu."User_id"
					left join "user_profile" uu on b._modifier_id = uu."User_id"
					left join "v_appr_result" r on b.id = r."SourceDataPk" and r."SourceTableName" = 'bundle_head'
					where b."TableName" = 'place_out_check_list'
					    and b."id" = :bh_id
	        		""";

	        Map<String, Object> head_info = this.sqlRunner.getRow(sql, paramMap);

	        sql = """
	        		select ir."CheckItem_id" as id
				    , ci."ItemGroup1" as group1
				    , ci."ItemGroup2" as group2
				    , ci."ItemGroup3" as group3
				    , ci."Name" as item_name
				    , ci."ResultType" as result_type
				    , ir."Result1" as result1
				    , ir."Result2" as result2
				    , ir."Result3" as result3
				    , ci._order as index_order
				    , ir."CheckResult_id"
				    , ir.id as check_result_id
				    , af.id as file_id
				from check_item_result ir
				inner join check_item ci on ci.id = ir."CheckItem_id"
				left join check_result cr on cr.id = ir."CheckResult_id"
				left join attach_file af on af."DataPk" = ci.id and "TableName" = 'check_item'
				where cr."SourceDataPk" = :bh_id
				order by ir._order
	        		""";

	        List<Map<String, Object>> diary_info = this.sqlRunner.getRows(sql, paramMap);

	        sql = """
        		select da.id, cir.id as check_result_id
				, da."HappenDate" as happen_date, da."HappenPlace" as happen_place
				, da."AbnormalDetail" as abnormal_detail, da."ActionDetail" as action_detail
				, da."ActionState" as action_state, da."ConfirmState" as confirm_state
				, da."SourceDataPk" as src_data_pk, da."SourceTableName" as src_table_name
				, up."Name" as actor_name
				, up2."Name" as creater_name
				, right(da."AbnormalDetail", strpos(':', reverse(da."AbnormalDetail"))-1) as check_name
				, cir._order as _order, ci._order as index_order
				from "devi_action" da
				inner join "check_item_result" cir on cir.id = da."SourceDataPk" and da."SourceTableName" = 'place_out_check_list'
				inner join "check_result" cr on cr.id = cir."CheckResult_id" and cr."SourceDataPk" = :bh_id
				inner join "check_item" ci on cir."CheckItem_id" = ci.id
				left join "user_profile" up on up."User_id" = da._modifier_id
				left join "user_profile" up2 on up2."User_id" = da._creater_id
				order by ci._order
	        		""";

	        List<Map<String, Object>> action_info = this.sqlRunner.getRows(sql, paramMap);

	        items.put("head_info", head_info);
	        items.put("diary_info", diary_info);
	        items.put("action_info", action_info);
	    }

	    return items;
	}
}
