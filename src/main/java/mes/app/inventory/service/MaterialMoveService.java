package mes.app.inventory.service;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import mes.domain.services.SqlRunner;

@Service
public class MaterialMoveService {

	@Autowired
	SqlRunner sqlRunner;
	
	public List<Map<String, Object>> getMaterialMoveList(Integer storehouse_id, Integer material_id, String keyword, String spjangcd) {
		MapSqlParameterSource param = new MapSqlParameterSource();		
		param.addValue("storehouse_id", storehouse_id);
		param.addValue("material_id", material_id);
		param.addValue("keyword", keyword);
		param.addValue("spjangcd", spjangcd);
		
		String sql = """
        with AA as(
        select m.id as mat_id, count(ml.id) as mat_lot_check
        from material m 
        left join mat_lot ml on m.id = ml."Material_id"
        where 1=1
        and m.spjangcd = :spjangcd
        group by m.id
        )
        select m.id as mat_id
        , fn_code_name('mat_type', mg."MaterialType") as mat_type
        , mg."Name" as mat_group
        , m."Code" as mat_code
        , m."Name" as material
        , sh.id as storehouse_id
        , sh."Name" as storehouse
        , un."Name" as unit_name
        , m."CurrentStock" as total_stock
        , mh."CurrentStock" as current_stock
        , count(*) over (partition by m.id) as house_count
        , AA.mat_lot_check as mat_lot_count
        , case when AA.mat_lot_check = 0 then 'N' else 'Y' end as mat_lot_check
        from material m 
        inner join mat_grp mg on mg.id = m."MaterialGroup_id"
        inner join AA on AA.mat_id = m.id
        left join mat_in_house mh on mh."Material_id" = m.id
        left join store_house sh on sh.id = mh."StoreHouse_id"
        left join unit un on un.id = m."Unit_id"
        where 1 = 1
        and m."Useyn" = '0'
		""";
		if(storehouse_id!=null) {
			sql+="""
			and sh.id = :storehouse_id
			""";
		}
		
		if(material_id!=null) {
			sql+="""
			and m.id = :material_id
			""";
		}
		
		if(StringUtils.hasText(keyword)){
			sql+="""
			and ( upper(m."Name") like concat('%%',upper(:keyword),'%%') or upper(m."Code") = upper(:keyword) )
			""";
		}

		sql+="""
		order by mg."MaterialType", mg."Name", m."Code", m."Name", m.id, sh."Name"
		""";

        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, param);
        return items;
	}

	public List<Map<String, Object>> getHouseMoveList(Integer storehouse_id, Integer mat_grp_pk, String keyword, String spjangcd) {
		MapSqlParameterSource param = new MapSqlParameterSource();
		param.addValue("storehouse_id", storehouse_id);
		param.addValue("mat_grp_pk", mat_grp_pk);
		param.addValue("keyword", keyword);
		param.addValue("spjangcd", spjangcd);

		String sql = """
        select 
            m.id as mat_id,
            fn_code_name('mat_type', mg."MaterialType") as mat_type,
            mg."Name" as mat_group,
            m."Code" as mat_code,
            m."Name" as material,
            sh.id as storehouse_id,
            sh."Name" as storehouse,
            m."CurrentStock" as total_stock,
            mh."CurrentStock" as current_stock,
            count(*) over (partition by m.id) as house_count,
            case when m."LotUseYN" = 'Y' then 'Y' else 'N' end as mat_lot_check,
            '' as cho_storehouse
        from material m
        inner join mat_grp mg on mg.id = m."MaterialGroup_id"
        left join mat_in_house mh on mh."Material_id" = m.id
        left join store_house sh on sh.id = mh."StoreHouse_id"
        where 1 = 1
          and m.spjangcd = :spjangcd
          and m."Useyn" = '0'
    """;

		if (storehouse_id != null) {
			sql += """
            and sh.id = :storehouse_id
        """;
		}

		if (mat_grp_pk != null) {
			sql += """
            and mg.id = :mat_grp_pk
        """;
		}

		if (StringUtils.hasText(keyword)) {
			sql += """
            and (
                upper(m."Name") like concat('%%', upper(:keyword), '%%') or
                upper(m."Code") = upper(:keyword)
            )
        """;
		}

		sql += """
        order by mg."MaterialType", mg."Name", m."Code", m."Name", m.id, sh."Name"
    """;

		return this.sqlRunner.getRows(sql, param);
	}



	public List<Map<String, Object>> getMaterialLotList(Integer storehouse_id, Integer material_id){
		MapSqlParameterSource param = new MapSqlParameterSource();
		param.addValue("storehouse_id", storehouse_id);
		param.addValue("material_id", material_id);

		String sql = """
        select m.id as mat_id
        , ml.id as mat_lot_id
        , ml."LotNumber"
        , u."Name" as unit_name
        , ml."InputQty"
        , ml."CurrentStock"
        , sh.id as storehouse_id
        , sh."Name" as storehouse_name
        from material m 
        inner join mat_lot ml on m.id = ml."Material_id" 
        inner join store_house sh on ml."StoreHouse_id" = sh.id
        inner join unit u on m."Unit_id" = u.id
        where m.id = :material_id
        and sh.id = :storehouse_id
        and ml."CurrentStock" > 0
		""";
		return this.sqlRunner.getRows(sql, param);
	}
	
	public int updateMaterialLotStorehouse(int ml_id, int storehouse_id) {
		MapSqlParameterSource param = new MapSqlParameterSource();
		param.addValue("ml_id", ml_id);
		param.addValue("storehouse_id", storehouse_id);
		String sql = """
        update mat_lot set "StoreHouse_id" =:storehouse_id where id=:ml_id
		""";
		return this.sqlRunner.execute(sql, param);
	}
}
