package mes.app.shipment.service;

import java.util.List;
import java.util.Map;

import mes.app.common.TenantContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import io.micrometer.core.instrument.util.StringUtils;
import mes.domain.services.SqlRunner;

@Service
public class ShipmentDoaService {

	@Autowired
	SqlRunner sqlRunner;
	
	public List<Map<String, Object>> getOrderList(String dateFrom, String dateTo, String notShip, String compPk,
			String matGrpPk, String matPk, String keyword) {
		String tenantId = TenantContext.get();
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("dateFrom", dateFrom);
		paramMap.addValue("dateTo", dateTo);
		paramMap.addValue("notShip", notShip);
		paramMap.addValue("compPk", compPk);
		paramMap.addValue("matGrpPk", matGrpPk);
		paramMap.addValue("matPk", matPk);
		paramMap.addValue("keyword", keyword);
		String state = null;
		if(notShip.equals("Y")) state = "ordered";
		paramMap.addValue("state", state);
		paramMap.addValue("spjangcd", tenantId);

        String sql = """
			    with SH as
			    (	select sh.id
	                , sh."Company_id" as company_id
	                , c."Name" as company_name
	                , sh."ShipDate" as ship_date
	                , sh."TotalQty" as total_qty
	                , sh."TotalPrice" as total_price
	                , sh."TotalVat" as total_vat
	                , sh."State" as state
	                , fn_code_name('shipment_state', sh."State") as state_name
		            , sh."Description" as description
	                from shipment_head sh
		            left join company c on c.id = sh."Company_id"
    		        where sh."ShipDate"  between cast(:dateFrom as date) and cast(:dateTo as date)
    		        and sh.spjangcd = :spjangcd 
                """;
        
        if (StringUtils.isEmpty(compPk)==false)  sql += " and sh.\"Company_id\" = cast(:compPk as Integer) ";
        if (StringUtils.isEmpty(state)==false)  sql += " and sh.\"State\" = :state ";
        
        sql += """
    		), S as 
		    (
		    select s."ShipmentHead_id" as head_id
            , sum(s."OrderQty") as tot_order_qty
            , sum(s."Qty") as tot_ship_qty
		    from SH
		    inner join shipment s on s."ShipmentHead_id" = SH.id 
            inner join material m on m.id = s."Material_id"
            where 1 = 1 
    		""";
        		
        if (StringUtils.isEmpty(matPk)==false)  sql += " and s.\"Material_id\" = cast(:matPk as Integer) ";
        if (StringUtils.isEmpty(matGrpPk)==false)  sql += " and m.\"MaterialGroup_id\"  = cast(:matGrpPk as Integer) ";
        if (StringUtils.isEmpty(keyword)==false)  sql += " and ( m.\"Name\" ilike concat('%%',:keyword,'%%') or m.\"Code\" ilike concat('%%',:keyword,'%%')) ";
        
        sql += """
			    group by s."ShipmentHead_id"
			    )
			    select SH.*
	            , S.tot_order_qty
	            , S.tot_ship_qty
			    from SH 
			    inner join S on S.head_id = SH.id
	            where 1 = 1
        		""";
		
        List<Map<String,Object>> items = this.sqlRunner.getRows(sql, paramMap);
		
		return items;
	}

	public List<Map<String, Object>> getShipmentItemList(String headId) {
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("headId", headId);
		
		String sql = """
			select s.id as ship_pk
			, s."Material_id" as mat_pk
			, mg."Name" as mat_grp_name
			, m."Code" as mat_code
			, m."Name" as mat_name
            , m."UnitPrice" as mat_unit_price
            , coalesce((s."OrderQty" * m."UnitPrice"), 0) as order_mat_price
			, u."Name" as unit_name
			, s."OrderQty" as order_qty 
			, s."Qty" as ship_qty
			, s."Description" as description 
			, s."UnitPrice" as unit_price
			, s."Price" as price 
			, s."Vat" as vat 
            , m."VatExemptionYN" as vat_exempt_yn
            , s."SourceDataPk" as src_data_pk
            , s."SourceTableName" as src_table_name
			from shipment  s
			inner join material m on m.id = s."Material_id" 
			inner join mat_grp mg on mg.id = m."MaterialGroup_id"
			left join unit u on u.id = m."Unit_id" 
            inner join shipment_head sh on sh.id = s."ShipmentHead_id"  
			where s."ShipmentHead_id" = cast(:headId as Integer)
            order by m."Code", m."Name"
				""";
		
        List<Map<String,Object>> items = this.sqlRunner.getRows(sql, paramMap);
		
		return items;
	}

}
