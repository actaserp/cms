package mes.app.shipment.service;

import java.sql.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import mes.app.common.TenantContext;
import mes.domain.entity.Suju;
import mes.domain.repository.SujuRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import io.micrometer.core.instrument.util.StringUtils;
import mes.domain.services.SqlRunner;

@Service
public class ShipmentOrderService {

	@Autowired
	SqlRunner sqlRunner;

	@Autowired
	SujuRepository sujuRepository;
	
	public List<Map<String, Object>> getSujuList(String dateFrom, String dateTo, String notShip, String compPk,
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
		paramMap.addValue("spjangcd", tenantId);

        String sql = """ 
    			with s as (
                select suju.id as suju_pk
	            , suju."JumunNumber" 
	            , suju."JumunDate"
	            , suju."DueDate"
	            , suju."Company_id"
	            --, suju."CompanyName"
                ,c2."Name" as "CompanyName" 
	            , fn_code_name('suju_state', suju."State") as "State"
	            , suju."Material_id"  
	            , suju."SujuQty" 
	            , suju."SujuQty2" 
                , suju."Description"
	            from suju suju
                inner join material m on m.id = suju."Material_id" 
                left join company c2 on c2.id = suju."Company_id"
	            where suju."JumunDate" between cast(:dateFrom as date) and cast(:dateTo as date) 
	            AND suju."State" NOT IN ('canceled', 'force_completion')
	            and suju.spjangcd = :spjangcd
                """;
        
        if (StringUtils.isEmpty(compPk)==false)  sql += " and suju.\"Company_id\" = cast(:compPk as Integer) ";
        if (StringUtils.isEmpty(matPk)==false)  sql += " and suju.\"Material_id\"  = cast(:matPk as Integer) ";
        if (StringUtils.isEmpty(matGrpPk)==false)  sql += " and m.\"MaterialGroup_id\"  = cast(:matGrpPk as Integer) ";
        sql += """
        		), SP as (
                select s.suju_pk
                , sum(RD."Number1") as order_sum
                , sum(sp."Qty") as ship_sum
                from S
                inner join rela_data RD on RD."DataPk1" = S.suju_pk
                and RD."TableName1" = 'suju'
                inner join shipment sp on sp.id = RD."DataPk2"
                and RD."TableName2" = 'shipment'
                group by s.suju_pk
            )
            select s.suju_pk, s."JumunNumber",s."JumunDate",s."DueDate"
            ,s."Company_id",s."CompanyName", s."Description" as remark
            , m.id as mat_id
            , fn_code_name('mat_type', mg."MaterialType") as mat_type
            , mg."Name" as mat_grp
            , m."Code" as mat_code
            , m."Name" as mat_name
            , s."SujuQty" as suju_qty
            , s."SujuQty2" as prod_qty 
            , sp.order_sum as order_qty 
            , sp.ship_sum  as shipment_qty
            , m."CurrentStock" as cur_stock	
            , u.id as unit_id
            , u."Name" as unit_name
            , case when sp.ship_sum > 0 then '출하' when sp.order_sum > 0 then '출하' else '' end as shipment_state
            , s."State"
            from s  
            left join sp on sp.suju_pk = s.suju_pk
            inner join material m on m.id = s."Material_id" 
            inner join mat_grp mg on mg.id = m."MaterialGroup_id"
            left join unit u on u.id = m."Unit_id" 
            where 1 = 1
           """;
        
        if (StringUtils.isEmpty(keyword)==false)  sql += " and (m.\"Name\" ilike concat('%%',:keyword,'%%') or m.\"Code\" ilike concat('%%',:keyword,'%%')) ";
        if ((notShip).equals("Y"))  sql += " and s.\"SujuQty\" > coalesce(sp.order_sum,0) ";
        sql += " order by s.\"DueDate\", s.\"CompanyName\", m.\"Code\", m.\"Name\"";
        
        List<Map<String,Object>> items = this.sqlRunner.getRows(sql, paramMap);
		
		return items;
	}

	public List<Map<String, Object>> getProductList(String matGrpPk, String matPk, String keyword) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("matGrpPk", matGrpPk);
		paramMap.addValue("matPk", matPk);
		paramMap.addValue("keyword", keyword);
		
        String sql = """ 
    			select m.id as mat_id
                , fn_code_name('mat_type', mg."MaterialType") as mat_type
                    , mg."Name" as mat_grp
                , m."Code" as mat_code
                , m."Name" as mat_name
                , u.id as unit_id
                , u."Name" as unit_name
                , m."CurrentStock" as cur_stock
                from material m
                left join mat_grp mg on mg.id = m."MaterialGroup_id" 
                left join unit u on u.id = m."Unit_id" 
                where mg."MaterialType" in ('product', 'semi')
                """;
	        if (StringUtils.isEmpty(matPk)==false)  sql += " and m.\"id\" = cast(:matPk as Integer) ";
	        if (StringUtils.isEmpty(matGrpPk)==false)  sql += " and mg.\"id\"  = cast(:matGrpPk as Integer) ";
	        if (StringUtils.isEmpty(keyword)==false)  sql += " and (m.\"Name\" ilike concat('%%',:keyword,'%%') or m.\"Code\" ilike concat('%%',:keyword,'%%')) ";
	        
	        sql += " order by mg.\"MaterialType\", m.\"Code\", m.\"Name\" ";
	        
	        List<Map<String,Object>> items = this.sqlRunner.getRows(sql, paramMap);
			
			return items;
	}
	
	// 출하지시 목록 조회
	public List<Map<String, Object>> getShipmentOrderList(String date_from, String date_to, String state, Integer comp_pk, Integer mat_grp_pk, Integer mat_pk, String keyword) {
		String tenantId = TenantContext.get();
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("date_from", Date.valueOf(date_from));
		paramMap.addValue("date_to", Date.valueOf(date_to));
		paramMap.addValue("state", state);
		paramMap.addValue("comp_pk", comp_pk);
		paramMap.addValue("mat_grp_pk", mat_grp_pk);
		paramMap.addValue("mat_pk", mat_pk);
		paramMap.addValue("keyword", keyword);
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
                where sh."ShipDate"  between :date_from and :date_to
                and sh.spjangcd = :spjangcd
				         """;
		if (comp_pk != null) {
			sql += " and sh.\"Company_id\" = :comp_pk ";
		}
		
		if (StringUtils.isEmpty(state) == false) {
			sql += "  and sh.\"State\" = :state ";
		}
		
		if (mat_pk != null || mat_grp_pk != null || StringUtils.isEmpty(keyword) == false) {
			sql += """
					and exists ( select 1
            		    from shipment s 
                        inner join material m on m.id = s."Material_id" 
                        left join mat_grp mg on mg.id = m."MaterialGroup_id"
                        where s."ShipmentHead_id" = sh.id 
					""";
			
			if (mat_pk != null) {
				sql += " and s.\"Material_id\" = :mat_pk ";
			}
			
			if (mat_grp_pk != null) {
				sql += " and mg.id = :mat_grp_pk ";
			}
			
			if (StringUtils.isEmpty(keyword) == false) {
				sql += """
						 and ( m."Name" ilike concat('%%', :keyword,'%%')
						       or m."Code" ilike concat('%%', :keyword,'%%'))
						""";
			}
			
			sql += """
					)
					       order by sh."ShipDate", c."Name", sh.id
					""";
		}

		List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);

		return items;
	}
	
	// 출하 품목 목록 조회
	public List<Map<String, Object>> getShipmentItemList (Integer head_id){

		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("head_id", head_id);

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
			where s."ShipmentHead_id" = :head_id
            order by m."Code", m."Name"
		 """;
		
		List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);

		return items;
	}

	public List<Suju> getRelationSujuList(List<Map<String, Object>> items){

		List<Integer> SuJuItems = items.stream()
				.map(item -> item.get("suju_pk"))
				.filter(val -> !val.toString().trim().isEmpty())
				.map(val -> Integer.parseInt(val.toString()))
				.collect(Collectors.toList());


		List<Suju> byIdIn = sujuRepository.findByIdIn(SuJuItems);

		System.out.println("Su");
		return byIdIn;
	}

	public List<Map<String, Object>> getProdcutList (Set<Integer> mat_id, Integer companyId){

		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("mat_id", mat_id);
		paramMap.addValue("company_id", companyId);

		String sql = """
				select distinct on ("Material_id") *
				from mat_comp_uprice
				where "Material_id" IN (:mat_id)
				AND "Type" = '02'
				AND "Company_id" = :company_id
				AND "ApplyEndDate" > CURRENT_DATE
				ORDER BY "Material_id", "ApplyStartDate" desc
		""";

		List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);

		return items;
	}
	
}
