package mes.app.schedule.service;

import java.sql.Date;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import mes.domain.services.SqlRunner;

@Service
public class MatOrderListService {

	@Autowired
	SqlRunner sqlRunner;
	
	// 자재발주내역 조회
	public List<Map<String, Object>> getMatOrderHistorylist(String date_kind, String date1, String date2, Integer company_id, Integer mat_group_id, Integer mat_id) {
		
		MapSqlParameterSource dicParam = new MapSqlParameterSource(); 
		dicParam.addValue("date1", Date.valueOf(date1));
		dicParam.addValue("date2", Date.valueOf(date2));
		dicParam.addValue("company_id", company_id);
		dicParam.addValue("mat_group_id", mat_group_id);
		dicParam.addValue("mat_id", mat_id);

        String sql = """
        		SELECT mo.id
	            , mo."OrderNumber"
	            , mo."AvailableStock"
	            , mo."OrderQty"
	            , mo."PackOrderQty"
	            , mo."UnitPrice"
	            , mo."TotalPrice"
	            , mo."InputPlanDate"
	            , mo."Description"
	            , to_char(mo."OrderDate",'yyyy-mm-dd') as "OrderDate"
	            , fn_code_name('mat_order_state', mo."State" ) as "StateName"
	            , mo."State"
	            , mo."Approver_id"
	            , to_char(mo."ApproveDateTime", 'yyyy-mm-dd hh24:mi') as "ApproveDateTime" 
	            , c."Name" as "Company_Name"
	            , m."Code" as "Material_Code"
	            , m."Name" as "Material_Name"
	            , mo."MaterialRequirement_id"
	            , to_char(mo._created, 'yyyy-mm-dd hh24:mi') as _created
	            , mo._creater_id
	            , mo._modified
	            , mo._modifier_id
	            , mo._status
	            , mo."Material_id"
	            , u."Name" as "UnitName"
	            , m."PackingUnitName"
	            , up."Name" as "ApproverName"
	            FROM mat_order mo
	            LEFT JOIN company c ON mo."Company_id" = c.id
	            LEFT JOIN material m ON m.id = mo."Material_id"
	            LEFT JOIN unit u ON m."Unit_id" = u.id
	            LEFT JOIN mat_requ mr on mr.id = mo."MaterialRequirement_id" -- and mr."ProductionDate" between :date1 AND :date2
	            left join user_profile up on up."User_id" = mo."Approver_id"
	            WHERE 1=1        		
        		""";
        
        if ("order".equals(date_kind)) {
        	//  발주일 기준
        	sql += " and mo.\"OrderDate\" between :date1 AND :date2 ";
        } else {
        	// 입고예정일 기준
        	sql += " and mo.\"InputPlanDate\" between :date1 AND :date2 ";
        }

        if (company_id != null) {
        	sql += " and mo.\"Company_id\"=:company_id ";
        }

        if (mat_group_id != null) {
        	sql += " and m.\"MaterialGroup_id\"=:mat_group_id ";
        }

        if (mat_id != null) {
        	sql += " and m.id=:mat_id ";
        }
        
        sql += " ORDER BY m.\"Name\", mo.\"OrderDate\" ";
        		
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);
        
        return items;
	}
}
