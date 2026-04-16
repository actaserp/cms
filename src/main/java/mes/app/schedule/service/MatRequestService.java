package mes.app.schedule.service;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import io.micrometer.core.instrument.util.StringUtils;
import mes.domain.services.SqlRunner;

@Service
public class MatRequestService {

	@Autowired
	SqlRunner sqlRunner;
	
	public List<Map<String, Object>> getMatRequestList(String cboMatGrp, String cboMaterial, String chkSearchDate, String srchEndDt,
			String srchStartDt) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("dateFrom", srchStartDt);
		paramMap.addValue("dateTo", srchEndDt);
		paramMap.addValue("matGrpPk", cboMatGrp);
		paramMap.addValue("matPk", cboMaterial);
			
		String sql = """
	           with A as (
                select  mr.id
                , '발주 요청' as "State"
                , mg."Name" as mat_grp_name
                , mr."Material_id", m."Code", m."Name"
                , u."Name" as "UnitName"
                , coalesce((select sum(mo."PackOrderQty") From mat_order mo 
                        where mo."MaterialRequirement_id" = mr.id
                        and mo."State" not in ('rejected')
                        ),0) as "OrderQty" /*기발주량*/
                , coalesce(m."AvailableStock",0) as "AvailableStock" /*가용재고*/
                , coalesce(m."SafetyStock",0) as "SafetyStock" /*안전재고*/
                , mr."RequestQty" /*발주요청량*/
                , m."PackingUnitName" /*포장단위*/
                , greatest(1, coalesce(m."PackingUnitQty",1)) as "PackingUnitQty" /*포장단위량*/
                , ceiling(mr."RequestQty"/greatest(1, coalesce(m."PackingUnitQty",1))) as "PackOrderReqQty"   /*발주요청량(포장)*/ 
                , coalesce(m."MinOrder",0)  as "MinOrder" /*최소발주량*/
                , coalesce(m."MaxOrder",0)  as "MaxOrder" /*최대발주량*/
                , coalesce( m."LeadTime",0) as "LeadTime" /*리드타임*/
                , current_date + coalesce(m."LeadTime",0)::integer as "InputPlanDate" /*입고예정일*/
                , greatest(1, coalesce(m."LotSize",1)) as "LotSize" 
                , mr."RequestDate"
                , m."UnitPrice"
                from mat_requ mr
                inner JOIN material m ON m.id = mr."Material_id"
                left join mat_grp mg on mg.id = m."MaterialGroup_id"
                left join unit u ON m."Unit_id" = u.id
                where mr."MaterialType" ='material'
                and mr."RequestQty" > 0
				""";
		
		if (chkSearchDate.equals("Y")) {
			sql += " and mr.\"RequestDate\" between cast(:dateFrom as date) AND cast(:dateTo as date) ";
		}
		if (StringUtils.isEmpty(cboMatGrp)==false)  sql += " and m.\"MaterialGroup_id\" = cast(:matGrpPk as Integer) ";
		if (StringUtils.isEmpty(cboMaterial)==false)  sql += " and m.id = cast(:matPk as Integer) ";
		
		sql += """
				), C as
	            (
	                SELECT A."Material_id" as mat_pk
	                , mcu."Company_id" as comp_pk
	                , mcu."UnitPrice" as uprice
	                , row_number() over (partition by A."Material_id" order by mcu."UnitPrice", mcu."Company_id") as r_idx
	                FROM A
	                INNER JOIN mat_comp_uprice mcu on mcu."Material_id" = A."Material_id" 
	                WHERE now()::date between mcu."ApplyStartDate"  and "ApplyEndDate" 
	            )
	            SELECT A.id, A."State"
	            , A.mat_grp_name
	            , A."Code" as mat_code, A."Name" as mat_name, A."Material_id" as mat_pk, A."UnitName"
	            , A."AvailableStock", A."SafetyStock" , A."RequestQty"
	            , A."PackingUnitName", A."PackingUnitQty", A."PackOrderReqQty"
	            , A."OrderQty"
	            , case when A."PackOrderReqQty" <= A."OrderQty" then null
	              else least( 
		            ceiling ( greatest(A."PackOrderReqQty" - coalesce(A."OrderQty",0), A."MinOrder") / A."LotSize") * A."LotSize"	, A."MaxOrder")
	              end as "AddQty"
	            , A."InputPlanDate"
	            , A."MinOrder"
	            , A."MaxOrder"
	            , A."LotSize"
	            , A."LeadTime"
	            , C.comp_pk as "Company_id"
	            , coalesce(C.uprice, A."UnitPrice") as "UnitPrice"
	            , A."UnitPrice" as mat_unit_price
	            , A."RequestDate"
	            FROM A
	            LEFT JOIN C on C.mat_pk = A."Material_id" 
	            and C.r_idx = 1   
	            ORDER BY A."Name", A."RequestDate"
				""";
		
        List<Map<String,Object>> items = this.sqlRunner.getRows(sql, paramMap);
		
		return items;
	}

	public List<Map<String, Object>> getCompanyByMat(String matPk, String compPk) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("matPk", matPk);
		paramMap.addValue("compPk", compPk);
		
		String sql = """
            with A as(
                 SELECT mcu.id
                   , mcu."Company_id" as comp_pk
		           , mcu."UnitPrice" as unit_price
		           , row_number() over (partition by mcu."Company_id"  order by mcu."ChangeDate" desc) as r_idx
	            FROM mat_comp_uprice mcu 
	            WHERE 1 = 1
                and mcu."Material_id" = cast(:matPk as Integer)            
              and current_date between mcu."ApplyStartDate" and  mcu."ApplyEndDate"
            )
            select A.comp_pk, C."Name" as comp_name, A.unit_price
            from A
	        inner join company c on A.comp_pk  = c.id 
            where A.r_idx = 1
				""";
		
        List<Map<String,Object>> items = this.sqlRunner.getRows(sql, paramMap);
		
		return items;
	}

	public Map<String, Object> getUnitPrice(String matPk, String compPk) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("matPk", matPk);
		paramMap.addValue("compPk", compPk);
		
		String sql = """
            with A as(
                 SELECT mcu.id
                   , mcu."Company_id" as comp_pk
	               , mcu."Material_id" as mat_pk
		           , mcu."UnitPrice" as unit_price
		           , row_number() over (partition by mcu."Material_id"  order by mcu."ChangeDate" desc) as r_idx
	            FROM mat_comp_uprice mcu 
	            left JOIN company c on mcu."Company_id"  = c.id 
	            WHERE 1=1
                and mcu."Company_id" = cast(:compPk as Integer)
                and mcu."Material_id" = cast(:matPk as Integer)             
              and current_date between mcu."ApplyStartDate" and  mcu."ApplyEndDate"
            )
            SELECT mat_pk, comp_pk, unit_price
            FROM A
            WHERE A.r_idx = 1
				""";
        Map<String,Object> items = this.sqlRunner.getRow(sql, paramMap);
		
		return items;
	}

}
