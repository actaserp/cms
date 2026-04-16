package mes.app.inventory.service;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import io.micrometer.core.instrument.util.StringUtils;
import mes.domain.services.SqlRunner;

@Service
public class MatStockTakeService {

	@Autowired
	SqlRunner sqlRunner;

	// 조회
	public List<Map<String, Object>> getMatStockTakeList(Integer house_pk, String mat_type, Integer mat_grp, String mat_name, String manage_level, String spjangcd) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("house_pk", house_pk);
		paramMap.addValue("mat_type", mat_type);
		paramMap.addValue("mat_grp", mat_grp);
		paramMap.addValue("mat_name", mat_name);
		paramMap.addValue("spjangcd", spjangcd);
		paramMap.addValue("manage_level", manage_level);
		
        String sql = """
        		with A as (
                    select m.id as id
                        , fn_code_name('mat_type', mg."MaterialType") as mat_type
                        , m."ManagementLevel" as manage_level
                        , mg."Name" as mat_grp_name
                        , m."Code" as mat_code, m."Name" as mat_name, u."Name" as unit_name
                        , mih."CurrentStock" as account_stock
                        , mih."StoreHouse_id" as house_pk
                    from material m 
                    left join unit u on u.id = m."Unit_id" 
                    left join mat_grp mg on mg.id = m."MaterialGroup_id" 
                    left join mat_in_house mih on mih."Material_id" = m.id 
                    where 1 = 1
                    and mih."StoreHouse_id" = :house_pk
                    and (m."LotUseYN" = 'N' or m."LotUseYN" is null)
                    and m."Useyn" = '0'
                    and m.spjangcd = :spjangcd
	            """;
        
        if (StringUtils.isEmpty(mat_type) == false) {
        	sql += " and mg.\"MaterialType\" = :mat_type ";
        }

        if (StringUtils.isEmpty(manage_level) == false) {
        	sql += " and m.\"ManagementLevel\" = :manage_level ";
        }
        
        if (mat_grp != null) {
        	sql += " and m.\"MaterialGroup_id\" = :mat_grp ";
        }
        
        if (StringUtils.isEmpty(mat_name) == false) {
        	sql += " and m.\"Name\" like concat('%%',:mat_name,'%%') ";
        }
        
        sql += """
        		), B as (
                    select A.id
                    , max(to_char(st."TakeDate" + st."TakeTime",'yy.mm.dd hh24:mi')||st."State") as last_take
                    from A 
                    inner join stock_take st on st."Material_id" = A.id
                    and st."StoreHouse_id" = A.house_pk
                    group by A.id
                )
                select A.id, A.mat_type, A.manage_level, A.mat_grp_name, A.mat_code, A.mat_name
                , A.unit_name, A.account_stock, A.house_pk
                , substring( B.last_take,1,14) as last_take_date
                , substring( B.last_take,15,10) as state
                from A 
                left join B on B.id = A.id
	            order by A.mat_type, A.mat_name
        		""";
        
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);
        
        return items;
	}
}
