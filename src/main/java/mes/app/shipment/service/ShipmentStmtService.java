package mes.app.shipment.service;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import mes.domain.services.SqlRunner;

@Service
public class ShipmentStmtService {

	@Autowired
	SqlRunner sqlRunner;
	
	// 거래명세서 발행용 출하품목 리스트
	public List<Map<String, Object>> getShipmentItemForStmtList(Integer head_id) {
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("head_id", head_id);
		
		String sql = """
				with A as (
	 			select s.id as ship_pk
				, s."Material_id" as mat_pk
				, mg."Name" as mat_grp_name
				, m."Code" as mat_code
				, m."Name" as mat_name
				, u."Name" as unit_name
				, s."OrderQty" as order_qty 
				, s."Qty" as ship_qty
				, s."Description" as description 
				, s."UnitPrice" as unit_price
				, s."Price" as price 
				, s."Vat" as vat 
	            , s."SourceDataPk" as src_data_pk
	            , s."SourceTableName" as src_table_name
	            , sh."Company_id" as comp_pk 
	            , sh."ShipDate" as ship_date
	            , m."VatExemptionYN" as vat_exempt_yn
                , m."UnitPrice" as mat_unit_price
				from shipment  s
				inner join material m on m.id = s."Material_id" 
				inner join mat_grp mg on mg.id = m."MaterialGroup_id"
				left join unit u on u.id = m."Unit_id" 
	            inner join shipment_head sh on sh.id = s."ShipmentHead_id"  
				where s."ShipmentHead_id" = :head_id
            ), B as (
            select A.ship_pk, A.mat_pk, A.mat_grp_name, A.mat_code, A.mat_name, A.unit_name
            , A.order_qty, A.ship_qty, A.description
            , mcu."UnitPrice" as unit_price
            , A.order_qty * mcu."UnitPrice" as price 
            from A 
            inner join mat_comp_uprice mcu on mcu."Company_id"  = A.comp_pk 
            and mcu."Material_id" = A.mat_pk
            and A.ship_date between mcu."ApplyStartDate" and mcu."ApplyEndDate" 
            )
            select A.ship_pk, A.mat_pk, A.mat_grp_name, A.mat_code, A.mat_name, A.unit_name
            , A.order_qty, A.ship_qty, A.description
            , coalesce(A.unit_price, B.unit_price, A.mat_unit_price) as unit_price
            , A.price as price 
            , A.vat as vat 
            , case when A.vat_exempt_yn = 'Y' then A.price else A.price * 1.1 end as price2 
            , A.vat_exempt_yn
            , A.src_data_pk, A.src_table_name
            from A 
            left join B on B.ship_pk = A.ship_pk 
            order by A.mat_code, A.mat_name 
				""";
        List<Map<String,Object>> items = this.sqlRunner.getRows(sql, paramMap);
		
		return items;
	}

	public List<Map<String, Object>> checkMatPrice_record(Integer company_id, Integer material_id) {
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("company_id", company_id);
		paramMap.addValue("material_id", material_id);

		String sql = """
				select * from mat_comp_uprice
				where "Company_id" = :company_id
				AND "Material_id" = :material_id
				AND "ApplyEndDate" > CURRENT_DATE
				ORDER BY "ApplyStartDate" desc
				limit 1;
				""";
		List<Map<String,Object>> items = this.sqlRunner.getRows(sql, paramMap);

		return items;
	}

	
}
