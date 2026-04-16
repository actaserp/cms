package mes.app.inventory.service;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import io.micrometer.core.instrument.util.StringUtils;
import mes.domain.services.SqlRunner;

@Service
public class MatMoveAService {

	@Autowired
	SqlRunner sqlRunner;
	
	// 창고이동 조회
	public List<Map<String, Object>> getMatMoveList(Integer storehouse, Integer mat_group, Integer material, String mat_name) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();        
		paramMap.addValue("storehouse", storehouse);  
		paramMap.addValue("mat_group", mat_group);  
		paramMap.addValue("material", material);  
		paramMap.addValue("mat_name", mat_name);
		
        String sql = """
        		select m.id as mat_id
                , fn_code_name('mat_type', mg."MaterialType") as mat_type
                , mg."Name" as mat_group
                , m."Code" as mat_code
                , m."Name" as material
                , sh.id as store_house_id
                , sh."Name" as storehouse
                , m."CurrentStock" as total_stock
                , m."PackingUnitQty" as pack_qty
                , mh."CurrentStock" as current_stock
                , count(*) over (partition by m.id) as house_count
                from material m 
                inner join mat_grp mg on mg.id = m."MaterialGroup_id"
                left join mat_in_house mh on mh."Material_id" = m.id
                left join store_house sh on sh.id = mh."StoreHouse_id"
                where 1 = 1        		
        		""";
        if (storehouse != null) {
        	sql += " and sh.id = :storehouse ";
        }
        
        if (mat_group != null) {
        	sql += " and mg.id = :mat_group ";
        }
        
        if (material != null) {
        	sql += " and m.id = :material ";
        }
        		
        if (StringUtils.isEmpty(mat_name) == false) {
        	sql +=" and m.\"Name\" ilike concat('%%',:mat_name,'%%') ";
        }

        sql += " order by mg.\"MaterialType\", mg.\"Name\", m.\"Code\", m.\"Name\", m.id, sh.\"Name\" ";
       
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);

        return items;
	}
	
}
