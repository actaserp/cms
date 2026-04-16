package mes.app.precedence.service;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import mes.domain.services.SqlRunner;

@Service
public class MatInoutSanitizerService {

	@Autowired
	SqlRunner sqlRunner;
	
	public List<Map<String, Object>> getMatInoutSanitizer(String srchStartDt, String srchEndDt, String keyword) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();        
		paramMap.addValue("dateFrom", srchStartDt);  
		paramMap.addValue("dateTo", srchEndDt);  
		paramMap.addValue("keyword", keyword);
		
		String sql = """
	            with lot_cnt (mio_id, lot_count) as(
                    select ml."SourceDataPk" as mio_id, count(ml."LotNumber") as lot_count from mat_lot ml where ml."SourceTableName" ='mat_inout' group by ml."SourceDataPk"
                ) select mi.id as mio_pk
                , fn_code_name('inout_type', mi."InOut") as inout
                , mi."Material_id"
                , mi."InputType" 
                , mi."OutputType" 
                , case when mi."InOut" = 'in' then fn_code_name('input_type', mi."InputType") 
	                when mi."InOut" = 'out' then fn_code_name('output_type', mi."OutputType") end as inout_type
                , case when mi."InOut" = 'in' then nullif(mi."InputQty", 0) 
	                when mi."InOut" = 'out' then nullif(mi."OutputQty", 0) end as qty
                , to_char(mi."InoutDate",'yyyy-MM-dd') as "InoutDate"
                , to_char(mi."InoutTime", 'HH:mm') as "InoutTime"
                , sh."Name" as "store_house_name"
                , m."Code" as "material_code"
                , m."Name" as "material_name"
                , m."CurrentStock" 
                , m."ValidDays"
                , m."LotSize"
                , m."PackingUnitQty"
                , mi."StoreHouse_id"
                , mih2."CurrentStock" as "HouseStock"
                , m."SafetyStock" 
                , nullif(mi."InputQty", 0) as "InputQty"
                , nullif(mi."OutputQty", 0) as "OutputQty"
                , u2."Name" as "unit_name"
                , mi."Description" 
                , fn_code_name('mat_type', mg."MaterialType") as material_type
                --, lot_cnt.lot_number
                from mat_inout mi 
                left join lot_cnt on lot_cnt.mio_id = mi.id
                inner join material m on mi."Material_id" = m.id
                left join mat_grp mg on mg.id = m."MaterialGroup_id"
                inner join store_house sh on mi."StoreHouse_id" = sh.id
                left join unit u2 on m."Unit_id" = u2.id 
                --left join mat_order mo on mi."MaterialOrder_id" = mo.id 
                --and m.id = mo."Material_id" 
                left join mat_in_house mih2 on mih2."Material_id"  = m.id
                and mih2."StoreHouse_id" = mi."StoreHouse_id"
                where 1 = 1
                and  mg."Code" = '소독제'
                --and sh."HouseType" = 'material'
                and mi."InoutDate" between cast(:dateFrom as date) and cast(:dateTo as date)
				""";
		
		if (!keyword.isEmpty()) {
			sql += " and upper(m.\"Name\") like concat('%%',upper(:keyword),'%%') ";
		}
		
		sql += " order by \"InoutDate\" desc, \"InoutTime\" desc ";
		
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);
        
        return items;
	}

	public Map<String, Object> getMatInoutSanitizerDetail(Integer mioPk) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("mioPk", mioPk);
		
		String sql = """
                select mi.id as mio_pk ,mg."MaterialType" as mat_type ,mg.id as mat_grp_pk ,m.id as mat_pk
                ,case when mi."InOut" = 'in' then mi."InputQty" 
                when mi."InOut" ='out' then mi."OutputQty" end as inout_qty
                ,mi."InOut" as mio_type ,mi."StoreHouse_id" as store_house_pk
                ,case when mi."InOut" = 'in' then mi."InputType" 
                when mi."InOut" ='out' then mi."OutputType" end as inout_type
                ,mi."Description" as description
                , to_char(mi."InoutDate",'yyyy-MM-dd') as inout_date
                from mat_inout mi 
                inner join material m on m.id = mi."Material_id" 
                inner join mat_grp mg on mg.id = m."MaterialGroup_id" 
                where mi.id = :mioPk
				""";
		
		Map<String, Object> item = this.sqlRunner.getRow(sql, paramMap);
		return item;
	}

}
