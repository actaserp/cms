package mes.app.schedule.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import mes.domain.services.SqlRunner;

@Service
public class MatRequSimulationService {

	@Autowired
	SqlRunner sqlRunner;
	
	//제품별 수주량 만큼 원부자재 소요량의 전체 레벨의 합계를 보여준다
	public Map<String, Object> getMatRequSimulationList(
			String prod_pks, String order_qtys, String base_date) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("prod_pks", prod_pks);
		paramMap.addValue("order_qtys", order_qtys);
		paramMap.addValue("base_date", base_date);
			
		String sql = """
	           with B1 as 
	        (
	        select B.prod_pk, B.mat_pk, sum(bom_requ_qty) as bom_qty 
	        from tbl_bom_detail_ext (:prod_pks, :order_qtys, :base_date) B 
	        where 1=1
            --and B.b_level = 1
	        group by B.prod_pk, B.mat_pk
	        )
	        select B1.mat_pk
            , mg."MaterialType" as mat_type
	        , fn_code_name('mat_type', mg."MaterialType") as mat_type_name
	        , mg."Name" as mat_group_name
	        , m."Code" as mat_code
	        , m."Name" as mat_name 
            , u."Name" as unit_name
	        , sum(B1.bom_qty) as requ_qty
	        , m."CurrentStock" as cur_stock
	        , m."AvailableStock" as available_stock 
	        from B1 
	        inner join material m on m.id = B1.mat_pk
	        inner join mat_grp mg on mg.id = m."MaterialGroup_id"		
            left join unit u on u.id = m."Unit_id"
            where 1 = 1
            -- and mg."MaterialType" in ('raw_mat', 'sub_mat')
	        group by B1.mat_pk, mg."MaterialType", mg."Name", m."Code", m."Name"
	        , m."CurrentStock", m."AvailableStock", u."Name"
	        order by mg."MaterialType", mg."Name", m."Code", m."Name"
				""";
		
        List<Map<String,Object>> rows = this.sqlRunner.getRows(sql, paramMap);
		
        List<Map<String, Object>> rawList = new ArrayList<>();
        List<Map<String, Object>> banList = new ArrayList<>();
        
        for (Map<String, Object> row : rows) {
            String matType = (String) row.get("mat_type");
			rawList.add(row);

//            if (matType.equals("raw_mat") || matType.equals("sub_mat")) {
//                rawList.add(row);
//            } else if (matType.equals("semi")) {
//                banList.add(row);
//            }
        }
        
        Map<String, Object> items = new HashMap<String, Object>();
        items.put("raw_list", rawList);
        items.put("ban_list", banList);
		return items;
	}
}
