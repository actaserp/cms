package mes.app.check.service;

import java.sql.Date;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;
import io.micrometer.core.instrument.util.StringUtils;
import mes.domain.services.SqlRunner;

@Service
public class CheckResultService {

	@Autowired
	SqlRunner sqlRunner;

	// 점검이력 - 조회
	public List<Map<String, Object>> getCheckResult(Integer check_master_id, String check_class_code, String date_from, String date_to) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("check_master_id", check_master_id);
		paramMap.addValue("check_class_code", check_class_code);
		paramMap.addValue("date_from", Date.valueOf(date_from));
		paramMap.addValue("date_to", Date.valueOf(date_to));
		
        String sql = """
        		select cr.id as id
	            , cm."Name" as check_name
	            , cr."CheckDate" as check_date
	            , cr."CheckTime" as check_time
	            , cr."TargetName" as target_name
	            , cr."CheckerName" as checker_name
	            , cr."Description" as description
	            , cr._created
                , fn_appr_state('check_result', cr.id) as appr_state
                from check_result cr
	            inner join check_mast cm on cm.id = cr."CheckMaster_id"
	            where cr."CheckDate" between :date_from and :date_to
        		""";
  		
	    if (check_master_id != null) {
	    	sql +=" and cr.\"CheckMaster_id\" = :check_master_id ";
	    } else if (StringUtils.isEmpty(check_class_code) == false) {
	    	sql +=" and cm.\"CheckClassCode\" = :check_class_code ";
	    }
	    
        sql += " order by cm.\"Name\", cr.\"CheckDate\" ";
       
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);

        return items;
	}

	// 점검이력 - 상세조회
	public Map<String, Object> getCheckResultDetail(Integer check_result_id) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("check_result_id", check_result_id);
		
        String sql = """
        		select cr.id as id_info
	            , cm."Name" as check_name_info
	            , cr."CheckDate" as check_date_info
	            , cr."CheckTime" as check_time_info
	            , cr."TargetName" as target_name_info
	            , cr."CheckerName" as checker_name_info
	            , cr."Description" as description_info
	            , cr._created
	            , cr._created
                , fn_appr_state('check_result', cr.id) as appr_state
                from check_result cr
                inner join check_mast cm on cm.id = cr."CheckMaster_id"
                where cr.id = :check_result_id
        		""";
  		       
        Map<String, Object> item = this.sqlRunner.getRow(sql, paramMap);

        return item;
	}

	// 점검표 - 조회
	public List<Map<String, Object>> getCheckItemResultlist(Integer check_result_id) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("check_result_id", check_result_id);
		
        String sql = """
        		select ir.id as id
			    , ci."ItemGroup1" as group1
			    , ci."ItemGroup2" as group2
			    , ci."ItemGroup3" as group3
			    , ci."Name" as check_item_name
			    , ci."ResultType" as result_type
			    , ir."Result1" as result1
			    , ir."Result2" as result2
			    , ir."Result3" as result3
			    --, cr."Description" as description
			    from check_item_result ir
			    inner join check_item ci on ci.id = ir."CheckItem_id"
			    --left join check_result cr on cr.id = ir."CheckResult_id"
			    where ir."CheckResult_id" = :check_result_id
			    order by ir._order
        		""";
       
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);

        return items;
	}

	public List<Map<String, Object>> checkDeviActionList(Integer dataPk) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();        
		paramMap.addValue("dataPk", dataPk); 
		
		String sql = """
				select da.id, da."HappenDate" as happen_date, da."HappenPlace" as happen_place
	            , da."AbnormalDetail" as abnormal_detail, da."ActionDetail" as action_detail
	            , da."ActionState" as action_state, da."ConfirmState" as confirm_state
	            , da."SourceDataPk" as src_data_pk, da."SourceTableName" as src_table_name
		        from devi_action da
	            inner join check_item_result cir on cir.id = da."SourceDataPk"
	            and da."SourceTableName" = 'check_item_result'
		        where 1=1
	            and cir."CheckResult_id" = :dataPk
		        order by cir.id
				""";
		
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);

        return items;
	}

}