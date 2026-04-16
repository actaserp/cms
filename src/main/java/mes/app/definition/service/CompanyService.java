package mes.app.definition.service;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import io.micrometer.core.instrument.util.StringUtils;
import mes.domain.services.SqlRunner;

@Service
public class CompanyService {

	@Autowired
	SqlRunner sqlRunner;
	
	//업체 목록 조회
	public List<Map<String, Object>> getCompnayList(String compType, String keyword, String spjangcd) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("comp_type", compType);
		/*paramMap.addValue("group_name", groupName);*/
		paramMap.addValue("keyword", keyword);
		paramMap.addValue("spjangcd", spjangcd);
		
		String sql = """
			select c.id as id
            , "Name" as name
            , "EngName" as eng_name
            , "Code" as comp_code
            , "Code2" as comp_code2
            , "CompanyType"  as company_type
            , fn_code_name('company_type', c."CompanyType") as company_type_name
            , "BusinessNumber" as business_number
            , "CEOName"  as ceo_name
            , "ZipCode"  as zip_code
            , "Address" as address
            , "TelNumber" as tel_number
            , "FaxNumber" as fax_number
            , "BusinessType" as business_type
            , "BusinessItem" as business_item
            , "Email" as email
            , "PurchaseSalesDeadline" as purchase_sales_deadline
            , "LastTradingDay" as last_trading_day
            , "OurManager" as our_manager
            , "SalesManager" as sales_manager
            , "SalesManagerPhone" as sales_manager_phone
            , "AccountManager" as account_manager
            , "AccountManagerPhone" as account_manager_phone
            , "TrandingBank" as tranding_bank
            , "AccountHolder" as account_holder
            , "AccountNumber" as account_number
            , "CreditLimitAmount" as credit_limit_amount
            , "PaymentCondition" as payment_condition
            , "Description" as description
            , "GroupName" as group_name
            , relyn as relyn
            from company c 
            where 1 = 1
            AND c.spjangcd = :spjangcd
			""";
		if (StringUtils.isEmpty(compType)==false) sql +="and c.\"CompanyType\" = :comp_type ";
		/*if (StringUtils.isEmpty(groupName)==false) sql +="and upper(c.\"GroupName\") like concat('%%',upper(:group_name),'%%')";*/
		if (StringUtils.isEmpty(keyword)==false) sql+="and upper(c.\"Name\") like concat('%%',upper(:keyword),'%%')";
		
		sql += "order by c.\"id\" desc";
		
		List<Map<String,Object>> items = this.sqlRunner.getRows(sql, paramMap);
		
		return items;
	}
	
	// 업체 상세정보 조회
	public Map<String, Object> getCompanyDetail(int companyId) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("company_id", companyId);
		
		String sql = """
			select id as id
            , "Name" as name
            , "EngName" as eng_name
            , "Code" as comp_code
            , "Code2" as comp_code2
            , "CompanyType"  as company_type
            , "BusinessNumber" as business_number
            , "CEOName"  as ceo_name
            , "ZipCode"  as zip_code
            , "Address" as address
            , "TelNumber" as tel_number
            , "FaxNumber" as fax_number
            , "BusinessType" as business_type
            , "BusinessItem" as business_item
            , "Email" as email
            , "PurchaseSalesDeadline" as purchase_sales_deadline
            , "LastTradingDay" as last_trading_day
            , "OurManager" as our_manager
            , "SalesManager" as sales_manager
            , "SalesManagerPhone" as sales_manager_phone
            , "AccountManager" as account_manager
            , "AccountManagerPhone" as account_manager_phone
            --, "CreditSalesLimitAmount" as credit_sales_limit_amount
            , "TrandingBank" as tranding_bank
            , "AccountHolder" as account_holder
            , "AccountNumber" as account_number
            , "CreditLimitAmount" as credit_limit_amount
            , "PaymentCondition" as payment_condition
            , "Description" as description
            , "Homepage" as homepage
            , "FirstTradingDay" as first_trading_day
            , "ReceivableAmount" as receivable_amount
            , "UnpaidAmount" as unpaid_amount
            , "ManageRemark" as manage_remark
            , "GroupName" as group_name
            , "invoiceEmail"
            , relyn as relyn
            , "IsPersonal"
            , "CorporateNumber"
            , "PrintTradingName" 
            from company c 
            Where id = :company_id
			""";
		
		Map<String, Object> item = this.sqlRunner.getRow(sql, paramMap);
		
		return item;
	}
	
	// 업체 단가정보
	public List<Map<String, Object>> getPriceListByCompany(int companyId) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("comp_id", companyId);
		
		String sql = """
			with A as 
            (
                select mcu.id 
                , mcu."Material_id" 
                , mcu."Company_id"
                , mcu."UnitPrice" 
                , mcu."FormerUnitPrice" 
                , mcu."ApplyStartDate"
                , mcu."ApplyEndDate"
                , mcu."ChangeDate"
                , mcu."ChangerName" 
                , mcu."Type"
                , row_number() over (partition by mcu."Company_id" order by mcu."ApplyStartDate" desc) as g_idx
                , now() between mcu."ApplyStartDate" and mcu."ApplyEndDate" as current_check
                , now() < mcu."ApplyStartDate" as future_check
                from mat_comp_uprice mcu 
                where mcu."Company_id" = :comp_id
            )
            select A.id as mcu_id
            , A."Material_id" as mat_id
            , fn_code_name('mat_type', mg."MaterialType") as mat_type_name
            , mg."Name" as mat_grp_name
            , m."Code" as mat_code
            , m."Name" as mat_name
            , u."Name" as unit_name
            , A."UnitPrice" as unit_price
            , A."FormerUnitPrice" as former_unit_price
            , A."ApplyStartDate"::date as apply_start_date
            , A."ApplyEndDate"::date as apply_end_date
            , A."ChangeDate" as change_date
            , A."ChangerName" as changer_name
            , A."Company_id"
            , A."Type" as type
            from A 
            inner join material m on m.id = A."Material_id"
            left join mat_grp mg on mg.id = m."MaterialGroup_id"
            left join unit u on u.id = m."Unit_id"
            where ( A.current_check = true or A.future_check = true or A.g_idx = 1)
            order by m."Name", A."ApplyStartDate" 
			""";
		
		List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);
		
		return items;
	}
	
	// 단가 상세 조회
	public Map<String, Object> getMaterialPriceDetail(int priceId) {
		
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("price_id", priceId);
		
		String sql = """
			select mcu.id as price_id
			, m."MaterialGroup_id"
            , mcu."Material_id" 
            , mcu."Company_id" 
            , mcu."UnitPrice"
            , "FormerUnitPrice"
            , mcu."ApplyStartDate" as "ApplyStartDate"
            , mcu."ApplyEndDate" as "ApplyEndDate"
            , mcu."Type" as type
            , m."Name" as mat_name
            from mat_comp_uprice mcu 
            inner join material m on m.id = mcu."Material_id" 
            where 1 = 1
            and mcu.id = :price_id
			""";
		
		Map<String, Object> item = this.sqlRunner.getRow(sql, dicParam);
        
        return item;
	}
	
	
	// 품목별 단가 히스토리 리스트 조회
	public List<Map<String, Object>> getPriceHistoryByComp(int companyId, String Code) {
		
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("comp_id", companyId);
		dicParam.addValue("code", Code);
		
		String sql = """
			select mcu.id 
            , mcu."Material_id" as mat_id
            , fn_code_name('mat_type', mg."MaterialType") as mat_type_name
            , mg."Name" as mat_grp_name
            , m."Code" as mat_code
            , m."Name" as mat_name
            , u."Name" as unit_name
            , mcu."UnitPrice" as unit_price
            , mcu."FormerUnitPrice" as former_unit_price
            , mcu."ApplyStartDate"::date as apply_start_date
            , mcu."ApplyEndDate"::date as apply_end_date
            , mcu."ChangeDate" as change_date
            , mcu."ChangerName" as changer_name 
            , mcu."Type" as type
            from mat_comp_uprice mcu 
            inner join material m on m.id = mcu."Material_id"
            left join mat_grp mg on mg.id = m."MaterialGroup_id"
            left join unit u on u.id = m."Unit_id"
            where 1=1
            and mcu."Company_id" = :comp_id
            and m."Code" = :code
            
            order by m."Name", mcu."ApplyStartDate" desc 
			""";
		
		List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);
		
		return items;
	}
}
