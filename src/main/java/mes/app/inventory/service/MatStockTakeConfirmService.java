package mes.app.inventory.service;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import io.micrometer.core.instrument.util.StringUtils;
import mes.domain.services.SqlRunner;

@Service
public class MatStockTakeConfirmService {

	@Autowired
	SqlRunner sqlRunner;

	// 조회
	public List<Map<String, Object>> getMatStockTakeConfirmList(Integer house_pk, String mat_name, String spjangcd) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("house_pk", house_pk);
		paramMap.addValue("mat_name", mat_name);
		paramMap.addValue("spjangcd", spjangcd);
		
        String sql = """
        		select st.id 
	            , fn_code_name('mat_type', mg."MaterialType") as mat_type
                , m.id mat_id
                , mg."Name" as mat_grp_name
	            , m."Code" mat_code
	            , m."Name" mat_name
	            , u."Name" unit_name
                , sh.id as store_house_id
	            , sh."Name" store_house_name
	            , st."AccountStock" account_stock
	            , st."RealStock" real_stock
	            , st."Gap" gap
                , m."UnitPrice" as unit_price
                , m."UnitPrice" * st."Gap" as gap_money
	            , to_char(st."TakeDate" + st."TakeTime", 'yy-mm-dd hh24:mi') take_date_time
	            , case st."State" when 'taked' then '조사'
		            else '확인' end state
	            , st."Description" description
                from stock_take st 
                inner join material m on m.id = st."Material_id" 
                left join mat_grp mg on mg.id = m."MaterialGroup_id" 
                left join unit u on u.id = m."Unit_id" 
                inner join store_house sh on sh.id = st."StoreHouse_id"
                where st."State" = 'taked'
                and (m."LotUseYN" != 'Y' or m."LotUseYN" is null)
                and m.spjangcd = :spjangcd
	            """;
        
        if (house_pk != null) {
        	sql += " and st.\"StoreHouse_id\" = :house_pk ";
        }
        
        if (StringUtils.isEmpty(mat_name) == false) {
        	sql += " and m.\"Name\" like concat('%%',:mat_name,'%%') ";
        }
        
        sql += " order by mat_type, mat_name ";
        
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);
        
        return items;
	}
}
