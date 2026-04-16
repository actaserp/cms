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
public class ShipmentListService {

	@Autowired
	SqlRunner sqlRunner;

	public List<Map<String, Object>> getShipmentHeadList(String dateFrom, String dateTo, String compPk, String matGrpPk, String matPk, String keyword, String state) {
		String tenantId = TenantContext.get();
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("dateFrom", dateFrom);
		paramMap.addValue("dateTo", dateTo);
		paramMap.addValue("compPk", compPk);
		paramMap.addValue("matGrpPk", matGrpPk);
		paramMap.addValue("matPk", matPk);
		paramMap.addValue("keyword", keyword);
		//String state = "shipped";
		paramMap.addValue("state", state);
		paramMap.addValue("spjangcd", tenantId);
		
		String sql = """
				select sh.id
		        , sh."Company_id" as company_id
                , c."Name" as company_name
		        , sh."ShipDate" as ship_date
		        , sh."TotalQty" as total_qty
	            , sh."TotalPrice" as total_price
	            , sh."TotalVat" as total_vat
	            , sh."Description" as description
                , sh."State" as state
                , fn_code_name('shipment_state', sh."State") as state_name
                , to_char(coalesce(sh."OrderDate",sh."_created") ,'yyyy-mm-dd') as order_date
                , sh."StatementIssuedYN" as issue_yn
                , sh."StatementNumber" as stmt_number 
                , sh."IssueDate" as issue_date
                from shipment_head sh 
                join company c on c.id = sh."Company_id"   
                where sh."ShipDate"  between cast(:dateFrom as date) and cast(:dateTo as date)
                and sh.spjangcd = :spjangcd
				""";
		
		if (StringUtils.isEmpty(compPk)==false)  sql += " and sh.\"Company_id\" = cast(:compPk as Integer) ";
		if (StringUtils.isEmpty(state)==false)  sql += " and sh.\"State\" = :state ";
		if (StringUtils.isEmpty(matPk)==false || StringUtils.isEmpty(matGrpPk)==false || StringUtils.isEmpty(keyword)==false) {
			sql += """
					and exists ( select 1
        		    from shipment s
                    inner join material m on m.id = s."Material_id"
                    left join mat_grp mg on mg.id = m."MaterialGroup_id"
                    where s."ShipmentHead_id" = sh.id
					""";
			if (StringUtils.isEmpty(matPk)==false)  sql += " and s.\"Material_id\"  = cast(:matPk as Integer) ";
			if (StringUtils.isEmpty(matGrpPk)==false)  sql += " and mg.id  = cast(:matGrpPk as Integer) ";
			if (StringUtils.isEmpty(keyword)==false)  sql += " and ( m.\"Name\" ilike concat('%%',:keyword,'%%') or m.\"Code\" ilike concat('%%',:keyword,'%%')) ";


		}
		sql += """ 
		 		order by sh."ShipDate" desc
		 		""";
        List<Map<String,Object>> items = this.sqlRunner.getRows(sql, paramMap);
		
		return items;
	}

	public List<Map<String, Object>> getShipmentItemList(String headId, Integer company_id) {
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("headId", headId);
		paramMap.addValue("companyId", company_id);

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
				, sh."Company_id" as company_id
				, m.id as material_id
				,case
					when s."SourceTableName" = 'product' then mcu."UnitPrice"
					else su."UnitPrice"
				end as unit_price
				
				,case
					when s."SourceTableName" = 'product' then 'N'
					else su."InVatYN"
				end as invatyn
			
				,TRUNC((
				                  CASE
				                    WHEN s."SourceTableName" = 'product' THEN mcu."UnitPrice" * s."Qty"
				                    WHEN su."InVatYN" = 'Y' THEN (su."UnitPrice" * (10.0 / 11)) * s."Qty"
				                    ELSE su."UnitPrice" * s."Qty"
				                  END
				                )::numeric, 2) AS price
				,case
					when s."SourceTableName" = 'product' then (mcu."UnitPrice" * s."Qty") * 0.1
					when su."InVatYN" = 'Y' then (su."UnitPrice" - (su."UnitPrice" * (10.0/11))) * s."Qty"
					else (su."UnitPrice" * s."Qty") * 0.1
				end as vat
	            , m."VatExemptionYN" as vat_exempt_yn
	            , s."SourceDataPk" as src_data_pk
	            , s."SourceTableName" as src_table_name
				from shipment  s
				inner join material m on m.id = s."Material_id" 
				inner join mat_grp mg on mg.id = m."MaterialGroup_id"
				left join unit u on u.id = m."Unit_id" 
	            inner join shipment_head sh on sh.id = s."ShipmentHead_id"  
	            left join (	
				             			select distinct on ("Material_id") "Material_id", "UnitPrice"
				             			from mat_comp_uprice
				         				WHERE "Type" = '02'
				             				AND "Company_id" = :companyId
				             				AND "ApplyEndDate" > CURRENT_DATE
				             			order by "Material_id", "ApplyStartDate" desc
				         				) mcu on mcu."Material_id" = s."Material_id" and s."SourceTableName" = 'product'
				left join suju su on su.id = s."SourceDataPk"	
				where s."ShipmentHead_id" = cast(:headId as Integer)
	            order by m."Code", m."Name"
				""";
        List<Map<String,Object>> items = this.sqlRunner.getRows(sql, paramMap);
		
		return items;
	}
}
