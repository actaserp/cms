package mes.app.inventory.service;

import java.util.List;
import java.util.Map;

import org.apache.groovy.parser.antlr4.util.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import mes.domain.services.SqlRunner;

@Service
public class MaterialCurrentStockService {
	@Autowired
	SqlRunner sqlRunner;
	
	// 재고 현황 조회 
	public List<Map<String, Object>> getMaterialCurrentStockList(String mat_type, Integer mat_grp_pk, String mat_name, Integer store_house_id, String spjangcd) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();  
		paramMap.addValue("mat_type", mat_type);
		paramMap.addValue("mat_grp_pk", mat_grp_pk);
		paramMap.addValue("mat_name", mat_name);
		paramMap.addValue("store_house_id", store_house_id);
		paramMap.addValue("spjangcd", spjangcd);
		
		String sql = """
			select m.id, fn_code_name('mat_type', mg."MaterialType") as mat_type_name
            , mg."Name" as mat_grp_name, m."Code" as mat_code, m."Name" as mat_name
            , m."UnitPrice" as unit_price 
            , u."Name" as unit_name
            , m."CurrentStock" * m."UnitPrice" as stock_price
            , case when m."PackingUnitQty" >0 then round((m."CurrentStock" / m."PackingUnitQty")::numeric, 0)
                else null end as box_qty
            , m."AvailableStock" as avail_stock
            , m."ReservationStock" as reserve_stock
            , m."CurrentStock" as cur_stock
            , sh."Name" as house_name, mh."CurrentStock" as house_stock
            , count(*) over (partition by m.id) as house_count
            , count(ml.id) as lot_count
            , coalesce(m."SafetyStock",0) as safety_stock
            , m."LotUseYN" as lot_use_yn
            from material m 
            inner join mat_grp mg on mg.id = m."MaterialGroup_id"
            left join unit u on u.id = m."Unit_id"
            left join mat_in_house mh on mh."Material_id" = m.id
            left join store_house sh on sh.id = mh."StoreHouse_id"
            left join mat_lot ml on ml."StoreHouse_id"  = sh.id and m.id = ml."Material_id" and ml."CurrentStock" > 0
            where 1 = 1
            and m."Useyn" = '0'
            and m."spjangcd" =:spjangcd
			""";
		if (StringUtils.isEmpty(mat_type)==false) sql +=" and mg.\"MaterialType\" = :mat_type ";
		if (mat_grp_pk != null) sql +=" and mg.\"id\" = :mat_grp_pk ";
		if (StringUtils.isEmpty(mat_name)==false) sql +=" and (m.\"Name\" like concat('%%',:mat_name,'%%') or m.\"Code\" like concat('%%',:mat_name,'%%') ) ";
		if (store_house_id != null) sql +=" and mh.\"StoreHouse_id\" =:store_house_id ";
		
		sql += " group by m.id,mg.\"MaterialType\" ,mg.\"Name\" ,u.\"Name\" ,sh.\"Name\" , mh.\"CurrentStock\", m.\"SafetyStock\", m.\"LotUseYN\"  ";
		
		sql += " order by mg.\"MaterialType\", mg.\"Name\", m.\"Code\", m.\"Name\", m.id, sh.\"Name\" ";
		
		List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);
		
		return items;
	}
}
