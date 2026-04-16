package mes.app.inventory.service;

import java.sql.Date;
import java.util.List;
import java.util.Map;

import mes.app.common.TenantContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import mes.domain.services.CommonUtil;
import mes.domain.services.SqlRunner;

@Service
public class LotStockTakeService {

	@Autowired
	SqlRunner sqlRunner;

	public List<Map<String, Object>> getMaterialStockList(String mat_type, Integer mat_grp, Integer company_id, String keyword, String spjangcd)
	{
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();  
		paramMap.addValue("mat_type", mat_type);
		paramMap.addValue("mat_grp", mat_grp);
		paramMap.addValue("keyword", keyword);
		paramMap.addValue("company_id", company_id);
		paramMap.addValue("spjangcd", spjangcd);
		
		
		String sql ="""
        with a as (
        select 
        mcu."Material_id", mcu."Company_id"
        from mat_comp_uprice mcu 
        inner join material m on m.id = mcu."Material_id" 
        inner join mat_grp mg on mg.id = m."MaterialGroup_id" 
        where 1=1
		""";
		
		if(StringUtils.hasText(mat_type)) {
			sql += """
			and mg."MaterialType" = :mat_type
			""";
		}
		
		if(mat_grp!=null) {
			sql += """
			and m."MaterialGroup_id" = :mat_grp
			""";
		}

		if(StringUtils.hasText(keyword)) {

		}

		if(company_id!=null) {
			sql+="""
			and mcu."Company_id" = :company_id
			""";
		}

		sql+="""
        group by mcu."Material_id", mcu."Company_id"
        )
        select
        distinct 
        m.id as mat_id
        , fn_code_name('mat_type', mg."MaterialType") as mat_type
        , mg."Name" as mat_grp
        , mg.id as mg_id
        , m."Code" as mat_code
        , m."Name" as mat_name
        , round(m."CurrentStock"::numeric, 2) as cur_stock
        , u."Name" as unit_name
        from material m 
        left join a on a."Material_id" = m.id
        left join company c on c.id = a."Company_id"
        inner join mat_grp mg on mg.id = m."MaterialGroup_id"
        inner join unit u on u.id = m."Unit_id"
        left join mat_lot ml on ml."Material_id" = m.id
        where 1=1 and m."LotUseYN"='Y'
        and "Useyn" = '0'
        and m.spjangcd = :spjangcd
		""";

		if(StringUtils.hasText(mat_type)) {
			sql += """
			and mg."MaterialType" = :mat_type
			""";
		}
		
		if(mat_grp!=null) {
			sql += """
			and m."MaterialGroup_id" = :mat_grp
		    """;
		}
		
		if(StringUtils.hasText(keyword)) {
			sql+="""
			and ( upper(m."Name") like concat('%%', upper(:keyword),'%%') 
					or upper(m."Code") like concat('%%', upper(:keyword),'%%')  
					or upper(ml."LotNumber") like concat('%%', upper(:keyword),'%%') 
				 )
			""";
		}
		
		if(company_id!=null) {
			sql+="""
			and c.id = :company_id
			""";
		}
		
		return this.sqlRunner.getRows(sql, paramMap);
	}
	
	
	public List<Map<String, Object>> getMaterialLotList(Integer materialId, Integer storehouseId){
		String tenantId = TenantContext.get();
		MapSqlParameterSource paramMap = new MapSqlParameterSource();  
		paramMap.addValue("material_id", materialId);
		paramMap.addValue("storehouse_id", storehouseId);
		paramMap.addValue("spjangcd", tenantId);

		String sql = """
		with A as (
		select ml.id as ml_id
		, ml."LotNumber"  
		, ml."Material_id" as mat_id
		, round(ml."CurrentStock"::numeric, 2) as "CurrentStock"
		, ml."StoreHouse_id" as storehouse_id
		, sh."Name" as storehouse_name
		, m."Name" as mat_name
		, m."Code" as mat_code
		, u."Name" as unit_name
		from mat_lot ml 
		inner join store_house sh on sh.id = ml."StoreHouse_id" 
		inner join material m on m.id = ml."Material_id"
		inner join unit u on u.id = m."Unit_id" 
		where 1=1
        and ml."Material_id" = :material_id
        and ml.spjangcd = :spjangcd
        """;
		
		if(storehouseId!=null) {
			sql+="""
			and ml."StoreHouse_id" = :storehouse_id 
			""";
		}
		sql+="""
		""";
		
		sql+="""
		), B as (
		SELECT A.ml_id, A.storehouse_id 
		, max(concat(to_char(slt."TakeDate",'yy.mm.dd'),' ', to_char(slt."TakeTime",'HH24:MM'), slt."State")) as last_take 
		from A 
		inner join stock_lot_take slt on slt."MaterialLot_id" = A.ml_id and slt."StoreHouse_id" = A.storehouse_id 
		group by A.ml_id, A.storehouse_id 
		)
		select A.ml_id
		, A."LotNumber"
		, A.mat_id
		, A."CurrentStock"
		, A.storehouse_id
		, A.storehouse_name 
		, A.mat_name, A.mat_code, A.unit_name
		, SUBSTRING(B.last_take, 1,14) as last_take_date
		, SUBSTRING(B.last_take, 15,10) as state 
		from A 
		left join B on B.ml_id = A.ml_id and B.storehouse_id = A.storehouse_id 
		where A."CurrentStock" != 0 or SUBSTRING( B.last_take, 15,10) is not null
		order by A.ml_id
		""";

		return this.sqlRunner.getRows(sql, paramMap);
	}
	
	
	public List<Map<String, Object>> searchLotList(Integer material_id, Integer storehouse_id, String lot_number){
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();  
		paramMap.addValue("material_id", material_id);
		paramMap.addValue("storehouse_id", storehouse_id);
		paramMap.addValue("lot_number", lot_number);
		
		String sql = """
        with A as (
        select 
        ml.id as ml_id
        , sh."Name" as storehouse_name
        , ml."StoreHouse_id" as storehouse_id 
        , ml."LotNumber" 
        , ml."CurrentStock" 
        from mat_lot ml
        inner join store_house sh on sh.id = ml."StoreHouse_id" 
        where 1=1
        and ml."LotNumber" = :lot_number
		""";
		
		if (material_id!=null) {
			sql+="""
			and ml."Material_id" = :material_id
			""";
		}
		
		if(storehouse_id!=null) {
			sql+="""
			and ml."StoreHouse_id" = :storehouse_id
			""";
		}
		
		sql+="""
		), B as (
        select 
        A.ml_id
        , A.storehouse_id 
        , max(concat(to_char(slt."TakeDate",'yy.mm.dd'),' ',to_char(slt."TakeTime",'HH24:MM'), slt."State")) as last_take 
        from A 
        inner join stock_lot_take slt on slt."MaterialLot_id" = A.ml_id and slt."StoreHouse_id" = A.storehouse_id 
        group by A.ml_id, A.storehouse_id 
        )
        select 
        A.ml_id
        , A.storehouse_name
        , A.storehouse_id
        , A."LotNumber"
        , A."CurrentStock"
        , SUBSTRING( B.last_take, 1,14) as last_take_date
        , SUBSTRING( B.last_take, 15,10) as state 
        from A 
        left join B on B.ml_id = A.ml_id and B.storehouse_id = A.storehouse_id 
        order by A.ml_id
		""";
		return this.sqlRunner.getRows(sql, paramMap);
	}
	
	
	public List<Map<String, Object>> getLotAdjustConfirmList(Integer storehouse_id, String keyword, String spjangcd){
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("storehouse_id", storehouse_id);
		paramMap.addValue("keyword", keyword);
		paramMap.addValue("spjangcd", spjangcd);

		String sql = """
        select slt.id 
        , fn_code_name('mat_type', mg."MaterialType") as mat_type
        , m.id mat_id
        , ml.id ml_id
        , mg."Name" as mat_grp_name
        , m."Code" mat_code
        , m."Name" mat_name
        , u."Name" unit_name
        , sh.id as store_house_id
        , sh."Name" store_house_name
        , slt."AccountStock" account_stock
        , slt."RealStock" real_stock
        , slt."Gap" gap
        , ml."LotNumber"
        , m."UnitPrice" as unit_price
        , m."UnitPrice" * slt."Gap" as gap_money
        , concat(to_char(slt."TakeDate",'yy-mm-dd'),' ', to_char(slt."TakeTime",'HH24:MM')) take_date_time
        , case slt."State" when 'taked' then '조사' else '확인' end state 
        , slt."Description" description
        from stock_lot_take slt 
        inner join mat_lot ml on ml.id = slt."MaterialLot_id" 
        left join material m on m.id = ml."Material_id" 
        left join mat_grp mg on mg.id = m."MaterialGroup_id" 
        left join unit u on u.id = m."Unit_id" 
        inner join store_house sh on sh.id = slt."StoreHouse_id"
        where 1=1
        and slt."State" = 'taked'
        and slt.spjangcd = :spjangcd
        """;
		if(storehouse_id != null) {
			sql+="""
			and slt."StoreHouse_id" = :storehouse_id
			""";
		}
		
		if(StringUtils.hasText(keyword)) {
			sql+=""" 
            and ( upper(m."Name") like concat('%%',upper(:keyword),'%%') 
					or upper(m."Code") like concat('%%',upper(:keyword),'%%') 
					or upper(ml."LotNumber") like concat('%%',upper(:keyword),'%%')
            )
            """;
		}
		return this.sqlRunner.getRows(sql, paramMap);
	}
	
	
	public List<Map<String, Object>> getSotckLotTakeHistoryList(String strDateFrom, String strDateTo, Integer storehouse_id, String mat_type, Integer mat_grp_pk, String keyword, String spjangcd){

		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		Date date_from = CommonUtil.trySqlDate(strDateFrom);
		Date date_to = CommonUtil.trySqlDate(strDateTo);
		paramMap.addValue("date_from", date_from);
		paramMap.addValue("date_to", date_to);		
		paramMap.addValue("storehouse_id", storehouse_id);
		paramMap.addValue("mat_type", mat_type);
		paramMap.addValue("mat_grp_pk", mat_grp_pk);
		paramMap.addValue("keyword", keyword);
		paramMap.addValue("spjangcd", spjangcd);
		
		String sql = """
        select slt.id  
        , sh."Name" as house_name
        , fn_code_name('mat_type', mg."MaterialType") as mat_type_name	
        , mg."Name" mat_grp_name
        , m."Code" as mat_code
        , m."Name" as mat_name
        , u."Name" as unit_name
        , ml."LotNumber"
        , slt."AccountStock" as account_stock
        , slt."RealStock" as real_stock
        , slt."Gap" as gap
        , m."UnitPrice" as unit_price
        , slt."Gap" * m."UnitPrice" as gap_price 
        , slt."Description" as description
        , up."Name" taker_name
        , concat(to_char(slt."TakeDate", 'yyyy-mm-dd'),' ', to_char(slt."TakeTime", 'HH24:MM')) take_date_time
        , up2."Name" confirmer_name
        , to_char(slt."ConfirmDateTime", 'yyyy-mm-dd HH24:MM') confirm_date_time
        , case slt."State" when 'taked' then '조사' else '확인' end state
        from stock_lot_take slt 
        inner join store_house sh on sh.id = slt."StoreHouse_id" 
        inner join mat_lot ml on ml.id = slt."MaterialLot_id" 
        inner join material m on m.id = ml."Material_id" 
        left join mat_grp mg on mg.id = m."MaterialGroup_id" 
        left join unit u on u.id = m."Unit_id" 
        left join user_profile up on up."User_id" = slt."Taker_id" 
        left join user_profile up2 on up2."User_id" = slt."Confirmer_id" 
        where slt."TakeDate" between :date_from and :date_to
        and slt.spjangcd = :spjangcd
		""";
		
		if (storehouse_id!=null) {
			sql+="""
			and slt."StoreHouse_id" = :storehouse_id	
			""";
		}
		
		if(StringUtils.hasText(mat_type)) {
			sql+="""
			and mg."MaterialType" = :mat_type
			""";
		}
		
		if(mat_grp_pk!=null) {
			sql+="""
			and m."MaterialGroup_id" = :mat_group_pk
			""";
		}
		
		if(StringUtils.hasText(keyword)) {
			sql+="""
			and ( upper(m."Name") like concat('%%',upper(:keyword),'%%') or upper(m."Code") = upper(:keyword) )
			""";
		}
		
		sql+="""
		order by sh."Name", slt."TakeDate", slt."TakeTime", m."Name"
		""";

		return this.sqlRunner.getRows(sql, paramMap);
	}
}