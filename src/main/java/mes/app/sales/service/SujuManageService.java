package mes.app.sales.service;

import java.util.List;
import java.util.Map;
import java.sql.Timestamp;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import mes.domain.services.SqlRunner;

@Service
public class SujuManageService {
	@Autowired
	SqlRunner sqlRunner;
	
	// 수주내역 조회
	public List<Map<String, Object>> getSujuList(Timestamp start, Timestamp end, String date_kind, boolean all_yn) {
		
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("date_kind", date_kind);
		dicParam.addValue("start", start);
		dicParam.addValue("end", end);
		
		String sql = """
			select sj.id as suju_id
            , sj."JumunNumber" as jumun_num
            , com."Name" as company_name
		    , mg."Name" as product_group
		    , mat."Code" as product_code
            , mat."Name" as product_name
		    , u."Name" as unit
		    , sj."AvailableStock" as available_stock
		    , sj."SujuQty" as suju_qty
		    , sj."ReservationStock" as reservation_qty
		    , sj."SujuQty2" as suju_qty2
            , sj."JumunDate" as jumun_date
            , sj."DueDate" as due_date
            , sj."ProductionPlanDate" as plan_date
            , fn_code_name('suju_state', sj."State") as state
            from suju sj
            inner join company com on com.id = sj."Company_id"
            inner join material mat on mat.id = sj."Material_id"
		    inner join unit u on u.id = mat."Unit_id"
            inner join mat_grp mg on mg.id = mat."MaterialGroup_id"
            where 1 = 1
			""";
		
		if (all_yn == true) {
			sql += " and sj.\"ProductionPlanDate\" is null ";
		}else {
			if (date_kind.equals("sales")) {
				sql += " and sj.\"JumunDate\" between :start and :end ";
			}
			if (date_kind.equals("delivery")) {
				sql += " and sj.\"DueDate\" between :start and :end ";
			}
		} 
  
		sql += " order by sj.\"JumunDate\" desc, sj.id ";
		
		List<Map<String,Object>> items = this.sqlRunner.getRows(sql, dicParam);
		
		return items;
	}
	
}
