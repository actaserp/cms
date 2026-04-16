package mes.app.maintenance.service;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import io.micrometer.core.instrument.util.StringUtils;
import mes.domain.services.SqlRunner;

@Service
public class EquipFailureService {

	@Autowired
	SqlRunner sqlRunner;

	/**
	 * 설비고장 리스트 조회
	 * @param start_date
	 * @param end_date
	 * @param equip_id
	 * @return
	 */
	public List<Map<String, Object>> getEquipFailureList(String start_date, String end_date, String equip_id) {
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("start_date", start_date);
		paramMap.addValue("end_date", end_date);
		paramMap.addValue("equip_id", equip_id);
		
        String sql = """
        select em.id 
        , eg."Name" as equipment_group_name
        , e."Code" as equipment_code
        , e."Name" as equipment_name
        , em."DataDate" as data_date
        , em."FailStartDate" as fail_start_date
        , em."FailEndDate" as fail_end_date
        , em."FailDescription" as fail_description
        , em."MaintStartDate" as maint_start_date
        , em."MaintEndDate" as maint_end_date
        , em."Description" as maint_description
        , em."MaintCost" as maint_cost
        , em."ServicerName" as maint_worker
        from equip_maint em 
        inner join equ e on e.id = em."Equipment_id" 
        left join equ_grp eg on eg.id = e."EquipmentGroup_id" 
        where cast(em."DataDate" as text) between :start_date and :end_date
        """;
        
		if (StringUtils.isEmpty(equip_id)==false) sql += "and e.id = cast(:equip_id as integer) ";
		
		sql += "order by em.\"DataDate\" desc, em.id";
		
		List<Map<String,Object>> items = this.sqlRunner.getRows(sql, paramMap);
		
		return items;
	}

	public Map<String, Object> getEquipmentMaintDetail(int id) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("id", id);
		
		String sql = """
        select em.id 
	    , em."Equipment_id" as equipment_id
	    , em."MaintType" as maint_type
	    , em."MaintStartDate" as maint_start_date
	    , em."MaintEndDate" as maint_end_date 
	    , to_char(em."MaintStartTime", 'HH24:mi') as maint_start_time
	    ,to_char( em."MaintEndTime", 'HH24:mi') as maint_end_time
	    , em."Description" as description 
	    , em."ServicerName" as service_name
	    , em."MaintCost" as maint_cost 
	    , em."FailDescription" as fail_description
	    , em."FailStartDate" as fail_start_date
	    , em."FailStartTime" as fail_start_time
	    , em."FailEndDate" as fail_end_date
	    , em."FailEndTime" as fail_end_time
	    , em."FailHr" as fail_hr
        from equip_maint em 
        where em.id = :id
        """;
		
		Map<String, Object> item = this.sqlRunner.getRow(sql, paramMap);
		return item;
	}
	
}
