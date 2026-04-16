package mes.app.inventory.service;

import java.sql.Date;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import io.micrometer.core.instrument.util.StringUtils;
import mes.domain.services.SqlRunner;

@Service
public class MatStockTakeHistoryService {

	@Autowired
	SqlRunner sqlRunner;

	// 조회
	public List<Map<String, Object>> getMatStockTakeHistoryList(String date_from, String date_to, Integer house_pk, String mat_name, String mat_type, String manage_level, Integer mat_group_pk, String spjangcd) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("date_from", Date.valueOf(date_from));
		paramMap.addValue("date_to", Date.valueOf(date_to));
		paramMap.addValue("house_pk", house_pk);
		paramMap.addValue("mat_name", mat_name);
		paramMap.addValue("mat_type", mat_type);
		paramMap.addValue("manage_level", manage_level);
		paramMap.addValue("mat_group_pk", mat_group_pk);
		paramMap.addValue("spjangcd", spjangcd);
        
        String sql = """
        		select st.id
		        , sh."Name" as house_name
		        , fn_code_name('mat_type', mg."MaterialType") as mat_type_name	
		        , mg."Name" as mat_grp_name
		        , m."Code" as mat_code
		        , m."Name" as mat_name 
                , m."ManagementLevel"
		        , u."Name" as unit_name
		        , st."AccountStock" account_stock
                , st."RealStock" real_stock
                , st."Gap" gap
                , m."UnitPrice" as unit_price 
                , st."Gap" * m."UnitPrice" as gap_price
                , st."Description" description
                , up."Name" taker_name
                , to_char(st."TakeDate" + st."TakeTime", 'yyyy-mm-dd hh24:mi') take_date_time
                , up2."Name" confirmer_name
                , to_char(st."ConfirmDateTime", 'yyyy-mm-dd hh24:mi') confirm_date_time
                , case st."State" when 'taked' then '조사' else '확인' end state
                from stock_take st
                inner join store_house sh on sh.id = st."StoreHouse_id" 
                inner join material m on m.id = st."Material_id" 
                left join unit u on u.id = m."Unit_id" 
                left join mat_grp mg on mg.id = m."MaterialGroup_id" 
                left join user_profile up on up."User_id" = st."Taker_id" 
                left join user_profile up2 on up2."User_id" = st."Confirmer_id" 
                where st."TakeDate" between :date_from and :date_to
                and (m."LotUseYN" != 'Y' or m."LotUseYN" is null)
                and st.spjangcd = :spjangcd
	            """;
        
        if (house_pk != null) {
        	sql += " and st.\"StoreHouse_id\" = :house_pk ";
        }

        if (StringUtils.isEmpty(mat_type) == false) {
        	sql += " and mg.\"MaterialType\" = :mat_type ";
        }
        
        if (StringUtils.isEmpty(manage_level) == false) {
        	sql += " and m.\"ManagementLevel\" = :manage_level ";
        }
        
        if (mat_group_pk != null) {
        	sql += " and m.\"MaterialGroup_id\" = :mat_group_pk ";
        }
        
        if (StringUtils.isEmpty(mat_name) == false) {
        	sql += " and ( m.\"Name\" like concat('%%',:mat_name,'%%') or m.\"Code\" like concat('%%',:mat_name,'%%')) ";
        }
        
    	sql += " order by sh.\"Name\", st.\"TakeDate\", st.\"TakeTime\", m.\"Name\" ";
    	
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);
        
        return items;
	}
	
}
