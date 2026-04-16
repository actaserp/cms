package mes.app.popup;


import lombok.extern.slf4j.Slf4j;
import mes.app.common.TenantContext;
import mes.domain.model.AjaxResult;
import mes.domain.services.DateUtil;
import mes.domain.services.SqlRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static mes.Encryption.EncryptionUtil.decrypt;

@Slf4j
@RestController
@RequestMapping("/api/popup")
public class PopupController {

	@Autowired
	SqlRunner sqlRunner;

	@Autowired
	PopupService popupService;

	@RequestMapping("/search_material")
	public AjaxResult getSearchMaterial(
			@RequestParam(value="material_type", required=false) String material_type,
			@RequestParam(value="material_group", required=false) Integer material_group,
			@RequestParam(value="keyword", required=false) String keyword,
			@RequestParam(value="spjangcd") String spjangcd
			) {
		AjaxResult result = new AjaxResult();

		String sql ="""
	            select 
	            m.id
	            , m."Code"
	            , m."Name"
	            , m."MaterialGroup_id"
	            , mg."Name" as group_name
	            , mg."MaterialType"
	            , sc."Value" as "MaterialTypeName"
	            , sc."Code" as "MaterialTypeCode"
	            , u."Name" as unit_name
	            , m."Mtyn" as mtyn
	            , m."WorkCenter_id"
				, m."Equipment_id"
				, m."VatExemptionYN"
				, m."Standard1" as "Spec"
	            from material m
	            left join unit u on m."Unit_id" = u.id
	            left join mat_grp mg on m."MaterialGroup_id" = mg.id
	            left join sys_code sc on mg."MaterialType" = sc."Code" 
	            and sc."CodeType" ='mat_type'
	            where 1=1 
	            AND "Useyn" ='0' 
	            and m."spjangcd" = :spjangcd
	    """;

		if (StringUtils.hasText(material_type)){
            sql+=""" 
            and mg."MaterialType" =:material_type
            """;
		}

		if(material_group!=null){
            sql+="""            		
            and mg."id" =:material_group
            """;
		}

		if(StringUtils.hasText(keyword)){
            sql+="""
            and (m."Name" ilike concat('%%',:keyword,'%%') or m."Code" ilike concat('%%',:keyword,'%%'))
            """;
		}
		;
		sql += "order by mg.\"Name\" , m.\"Name\" ";

		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("material_type", material_type);
		paramMap.addValue("material_group", material_group, java.sql.Types.INTEGER);
		paramMap.addValue("keyword", keyword);
		paramMap.addValue("spjangcd", spjangcd);
		result.data = this.sqlRunner.getRows(sql, paramMap);
		return result;
	}


	@RequestMapping("/search_equipment")
	public AjaxResult getSearchMaterial(
			@RequestParam(value="group_id", required=false) Integer equipment_group,
			@RequestParam(value="keyword", required=false) String keyword
			) {
		AjaxResult result = new AjaxResult();

		String sql ="""
	            select 
                 e.id
                 , e."Code"
                 , e."Name"
                 , eg."Name" as group_name
                 , eg."EquipmentType"
                 , fn_code_name('equipment_type',  eg."EquipmentType") as "EquipmentTypeName"
                from equ e
                  left join equ_grp eg on e."EquipmentGroup_id" = eg.id
                where 1=1  
	    """;

		if(equipment_group!=null){
            sql+="""            		
            and e."EquipmentGroup_id"=:equipment_group
            """;
		}

		if(StringUtils.hasText(keyword)){
            sql+="""
            and upper(e."Name") like concat('%%',:keyword,'%%')
            """;
		}

		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("equipment_group", equipment_group, java.sql.Types.INTEGER);
		paramMap.addValue("keyword", keyword);

		result.data = this.sqlRunner.getRows(sql, paramMap);

		return result;
	}

	@RequestMapping("/pop_prod_input/mat_list")
	public AjaxResult getMatList(
			@RequestParam(value="mat_type", required=false) String matType,
			@RequestParam(value="mat_grp_id", required=false) Integer matGrpId,
			@RequestParam(value="keyword", required=false) String keyword,
			@RequestParam(value="jr_pk", required=false) Integer jrPk,
			@RequestParam(value="bom_comp_yn", required=false) String bomCompYn) {

		AjaxResult result = new AjaxResult();

		Timestamp today = DateUtil.getNowTimeStamp();

		String sql = "";

		if (bomCompYn.equals("Y")) {
			sql = """
				 select m.id, m."Code" as mat_code, m."Name" as mat_name
                , m."MaterialGroup_id" as mat_grp_id, mg."Name" as mat_grp_name
                , mg."MaterialType" as mat_type
                , sc."Value" as mat_type_name
                , u."Name" as unit_name
                from job_res jr
                inner join tbl_bom_detail(jr."Material_id"::text, cast(to_char(cast(:today as date),'YYYY-MM-DD') as text)) a  on a.b_level = 1 
                inner join material m on m.id = a.mat_pk
                left join unit u on m."Unit_id" = u.id
                left join mat_grp mg on m."MaterialGroup_id" = mg.id
                left join sys_code sc on mg."MaterialType" = sc."Code" 
                and sc."CodeType" ='mat_type'
                where jr.id =  :jrPk
                and m."LotUseYN" = 'Y'
				 """;

			MapSqlParameterSource paramMap = new MapSqlParameterSource();
			paramMap.addValue("jrPk", jrPk);
			paramMap.addValue("today", today);

			result.data = this.sqlRunner.getRows(sql, paramMap);

		} else {
			sql = """
					select m.id, m."Code" as mat_code, m."Name" as mat_name
	                , m."MaterialGroup_id" as mat_grp_id, mg."Name" as mat_grp_name
	                , mg."MaterialType" as mat_type
	                , sc."Value" as mat_type_name
	                , u."Name" as unit_name
	                from material m
	                left join unit u on m."Unit_id" = u.id
	                left join mat_grp mg on m."MaterialGroup_id" = mg.id
	                left join sys_code sc on mg."MaterialType" = sc."Code" 
	                and sc."CodeType" ='mat_type'
	                where 1=1
	                and m."LotUseYN" = 'Y'
				  """;

			if(!matType.isEmpty()) sql += "and mg.\"MaterialType\" = :matType ";
			if(matGrpId != null) sql += "and mg.\"id\" = :matGrpId ";
			if(!keyword.isEmpty()) sql += " and m.\"Name\" like concat('%%',:keyword,'%%') ";

			MapSqlParameterSource paramMap = new MapSqlParameterSource();
			paramMap.addValue("matType", matType);
			paramMap.addValue("matGrpId", matGrpId);
			paramMap.addValue("keyword", keyword);

			result.data = this.sqlRunner.getRows(sql, paramMap);
		}


		return result;
	}

	@RequestMapping("/pop_prod_input/mat_lot_list")
	public AjaxResult getMatLotList(
			@RequestParam(value="mat_pk", required=false) Integer matPk,
			@RequestParam(value="jr_pk", required=false) Integer jrPk) {

		AjaxResult result = new AjaxResult();

		String sql = """
		        with aa as (
		        	select mpi."MaterialLot_id" as mat_lot_id from job_res jr 
			        inner join mat_proc_input mpi on jr."MaterialProcessInputRequest_id" = mpi."MaterialProcessInputRequest_id" 
			        where jr.id = :jrPk
		        )
		       	select 
				a.id, m."Name" as mat_name, a."LotNumber" as lot_number
		        , a."CurrentStock" as cur_stock
		        , a."InputQty" as first_qty
		        , sh."Name" as storehouse_name
		        , to_char(a."EffectiveDate",'yyyy-mm-dd') as effective_date
		        , to_char(a."InputDateTime",'yyyy-mm-dd') as create_date
		        , case when aa.mat_lot_id is not null then 'Y' else 'N' end as lot_use
		        from mat_lot a
		        inner join material m on m.id = a."Material_id"
		        left join aa on aa.mat_lot_id = a.id
		        left join store_house sh on sh.id = a."StoreHouse_id" 
		        where a."Material_id" = :matPk
		        and a."CurrentStock" > 0
		        order by a."EffectiveDate" , a."InputDateTime" 
				""";

		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("matPk", matPk);
		paramMap.addValue("jrPk", jrPk);

		result.data = this.sqlRunner.getRows(sql, paramMap);

		return result;
	}

	@RequestMapping("/pop_prod_input/lot_info")
	public AjaxResult getLotInfo(
			@RequestParam(value="lot_number", required=false) String lotNumber) {

		AjaxResult result = new AjaxResult();

		String sql = """
			select a.id
			, mg."Name" as mat_grp_name
			, m."Name" as mat_name
	        , a."CurrentStock" as cur_stock
	        , a."InputQty" as first_qty
	        , sh."Name" as storehouse_name
		    , to_char(a."EffectiveDate",'yyyy-mm-dd') as effective_date
		    , to_char(a."InputDateTime",'yyyy-mm-dd') as create_date
		    from mat_lot a
		    inner join material m on m.id = a."Material_id" 
		    left join store_house sh on sh.id = a."StoreHouse_id"
		    left join mat_grp mg on mg.id = m."MaterialGroup_id" 
		    where a."LotNumber" = :lotNumber
			""";

		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("lotNumber", lotNumber);

		result.data = this.sqlRunner.getRow(sql, paramMap);

		return result;
	}

	@RequestMapping("/search_approver/read")
	public List<Map<String, Object>> getSearchApprover(
			@RequestParam(value="depart_id", required=false) Integer depart_id,
			@RequestParam(value="keyword", required=false) String keyword) {

		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("depart_id", depart_id);
		paramMap.addValue("keyword", keyword);

		String sql = """
				select up."User_id"
		        ,up."Name"
		        ,up."Depart_id" 
		        ,d."Name" as "DepartName"
		        from user_profile up 
		        left join depart d on d.id = up."Depart_id"
	            where 1=1 
				""";

		if (keyword != null) {
			sql += " and upper(up.\"Name\") like concat('%%',upper(:keyword),'%%') ";
        }

		if (depart_id != null) {
        	sql += " and up.\"Depart_id\" = :depart_id ";
        }

    	sql += " order by COALESCE(d.\"Name\",'Z') , up.\"Name\" ";


    	List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);

		return items;
	}

	@RequestMapping("/search_user_code/read")
	public List<Map<String, Object>> getSearchUserCode(
			@RequestParam(value="parent_code", required=false) String parentCode){

		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("parentCode", parentCode);

		String sql = """
	            select c.id, c."Code", c."Value", c."Description"
	            from user_code c
	            where exists (
		            select 1
		            from user_code
		            where "Code" = :parentCode
		            and "Parent_id" is null
		            and c."Parent_id" = id
	            )
	            order by _order
				""";

    	List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);

		return items;

	}

	@RequestMapping("/search_Comp")
	public AjaxResult getSearchComp(
			@RequestParam(value = "compCode", required = false) String compCode,
			@RequestParam(value = "compName", required = false) String compName,
			@RequestParam(value = "business_number", required = false) String business_number){

		String spjangcd = TenantContext.get();
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("compCode", compCode);
		paramMap.addValue("spjangcd", spjangcd);
		paramMap.addValue("compName", compName);
		paramMap.addValue("business_number", business_number);
		AjaxResult result = new AjaxResult();

		String sql = """
            select id as id
            , "Name" as compName
            , "Code" as compCode
            , "BusinessNumber" as business_number
            , "TelNumber" as tel_number
            , "CEOName" as invoiceeceoname
            , "Address" as invoiceeaddr
            , "BusinessType" as invoiceebiztype
            , "BusinessItem" as invoiceebizclass
            , "AccountManager" as invoiceecontactname1
            , "AccountManagerPhone" as invoiceetel1
            , "Email" as invoiceeemail1
            from company
            WHERE ("CompanyType" = 'sale'
            OR "CompanyType" = 'sale-purchase')
            AND ("relyn" = '0' OR "relyn" IS NULL)
            and spjangcd = :spjangcd
			""";

		if (compCode != null && !compCode.isEmpty()) {
			sql += " AND \"Code\" ILIKE :compCode ";
			paramMap.addValue("compCode", "%" + compCode + "%");
		}

		if (compName != null && !compName.isEmpty()) {
			sql += " AND \"Name\" ILIKE :compName ";
			paramMap.addValue("compName", "%" + compName + "%");
		}

		if (business_number != null && !business_number.isEmpty()) {
			sql += " AND \"BusinessNumber\" ILIKE :business_number ";
			paramMap.addValue("business_number", "%" + business_number + "%");
		}

		sql += " ORDER BY \"Name\" ASC ";

		result.data = this.sqlRunner.getRows(sql, paramMap);

		return result;
	}

	@RequestMapping("/search_acc_sub")
	public AjaxResult getSearchAccSubject(
			@RequestParam(value = "srchCode", required = false) String srchCode,
			@RequestParam(value = "srchName", required = false) String srchName,
			@RequestParam(value = "spjangcd") String spjangcd){

		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("srchCode", srchCode);
		paramMap.addValue("srchName", srchName);
		paramMap.addValue("spjangcd", spjangcd);
		AjaxResult result = new AjaxResult();

		String sql = """
            select *
            from tb_accsubject
            WHERE 1=1
            and "useyn" = 'Y'
			""";

		if (srchCode != null && !srchCode.isEmpty()) {
			sql += " AND \"acccd\" ILIKE :srchCode ";
			paramMap.addValue("srchCode", "%" + srchCode + "%");
		}

		if (srchName != null && !srchName.isEmpty()) {
			sql += " AND \"accnm\" ILIKE :srchName ";
			paramMap.addValue("srchName", "%" + srchName + "%");
		}

		sql += " ORDER BY \"acccd\" ASC ";

		result.data = this.sqlRunner.getRows(sql, paramMap);

		return result;
	}

	@RequestMapping("/search_project")
	public AjaxResult getSearchProject(
			@RequestParam(value = "srchCode", required = false) String srchCode,
			@RequestParam(value = "srchName", required = false) String srchName,
			@RequestParam(value = "spjangcd") String spjangcd){

		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("srchCode", srchCode);
		paramMap.addValue("srchName", srchName);
		paramMap.addValue("spjangcd", spjangcd);
		AjaxResult result = new AjaxResult();

		String sql = """
            select *
            from TB_DA003
            WHERE spjangcd = :spjangcd
			""";

		if (srchCode != null && !srchCode.isEmpty()) {
			sql += " AND \"projno\" ILIKE :srchCode ";
			paramMap.addValue("srchCode", "%" + srchCode + "%");
		}

		if (srchName != null && !srchName.isEmpty()) {
			sql += " AND \"projnm\" ILIKE :srchName ";
			paramMap.addValue("srchName", "%" + srchName + "%");
		}

		sql += " ORDER BY \"stdate\" DESC ";

		result.data = this.sqlRunner.getRows(sql, paramMap);

		return result;
	}

	@RequestMapping("/search_deduction")
	public AjaxResult getSearchDeduction(
			@RequestParam(value = "srchCode", required = false) String srchCode,
			@RequestParam(value = "srchName", required = false) String srchName
//			@RequestParam(value = "spjangcd") String spjangcd
	){

		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("srchCode", srchCode);
		paramMap.addValue("srchName", srchName);
//		paramMap.addValue("spjangcd", spjangcd);
		AjaxResult result = new AjaxResult();

		String sql = """
            select *
            from vat_deduction_type
            where 1=1
			""";

		if (srchCode != null && !srchCode.isEmpty()) {
			sql += " AND \"code\" ILIKE :srchCode ";
			paramMap.addValue("srchCode", "%" + srchCode + "%");
		}

		if (srchName != null && !srchName.isEmpty()) {
			sql += " AND \"name\" ILIKE :srchName ";
			paramMap.addValue("srchName", "%" + srchName + "%");
		}

		sql += " ORDER BY \"code\" ";

		result.data = this.sqlRunner.getRows(sql, paramMap);

		return result;
	}

	@RequestMapping("/search_expense")
	public AjaxResult getSearchExpense(
			@RequestParam(value = "srchCode", required = false) String srchCode,
			@RequestParam(value = "srchName", required = false) String srchName,
			@RequestParam(value = "spjangcd") String spjangcd){

		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("srchCode", srchCode);
		paramMap.addValue("srchName", srchName);
		paramMap.addValue("spjangcd", spjangcd);
		AjaxResult result = new AjaxResult();

		String sql = """
            SELECT
				 tc.*,
				 sc."Value" AS gartnm
			 FROM tb_ca648 tc
			 LEFT JOIN sys_code sc
				 ON sc."CodeType" = 'gartcd'
				AND sc."Code" = tc.gartcd
			   where 1=1
			""";

		if (srchCode != null && !srchCode.isEmpty()) {
			sql += " AND \"artcd\" ILIKE :srchCode ";
			paramMap.addValue("srchCode", "%" + srchCode + "%");
		}

		if (srchName != null && !srchName.isEmpty()) {
			sql += " AND \"artnm\" ILIKE :srchName ";
			paramMap.addValue("srchName", "%" + srchName + "%");
		}

		sql += " ORDER BY CAST(tc.gartcd AS INTEGER), CAST(tc.artcd AS INTEGER)";

		result.data = this.sqlRunner.getRows(sql, paramMap);

		return result;
	}

	@RequestMapping("/search_Comp_Custom")
	//@DecryptField(columns = "item2", masks = 0)
	public AjaxResult getSearchCompCustom(@RequestParam String spjangcd,
										  @RequestParam(required = false) String item,
										  @RequestParam(required = false) String item2){

		AjaxResult result = new AjaxResult();

        result.data = popupService.getCltCombineList(spjangcd, item, item2);
		
		return result;
	}

	@RequestMapping("/search_depart")
	public AjaxResult getSearchDepart(
			@RequestParam(value = "srchCode", required = false) String srchCode,
			@RequestParam(value = "srchName", required = false) String srchName,
			@RequestParam(value = "spjangcd") String spjangcd){

		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("srchCode", srchCode);
		paramMap.addValue("srchName", srchName);
		paramMap.addValue("spjangcd", spjangcd);
		AjaxResult result = new AjaxResult();

		String sql = """
            select *
            from depart
            WHERE spjangcd = :spjangcd
			""";

		if (srchCode != null && !srchCode.isEmpty()) {
			sql += " AND \"Code\" ILIKE :srchCode ";
			paramMap.addValue("srchCode", "%" + srchCode + "%");
		}

		if (srchName != null && !srchName.isEmpty()) {
			sql += " AND \"Name\" ILIKE :srchName ";
			paramMap.addValue("srchName", "%" + srchName + "%");
		}

		sql += " ORDER BY \"Code\" ASC ";

		result.data = this.sqlRunner.getRows(sql, paramMap);

		return result;
	}


	@RequestMapping("/get_Comp")
	public AjaxResult getSearchComp(
			@RequestParam(value = "id", required = false) Integer id
	){

		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("id", id);
		AjaxResult result = new AjaxResult();

		String sql = """
            select id as id
            , "Name" as compName
            , "Code" as compCode
            , "BusinessNumber" as business_number
            , "TelNumber" as tel_number
            , "CEOName" as invoiceeceoname
            , "Address" as invoiceeaddr
            , "BusinessType" as invoiceebiztype
            , "BusinessItem" as invoiceebizclass
            , "AccountManager" as invoiceecontactname1
            , "AccountManagerPhone" as invoiceetel1
            , "Email" as invoiceeemail1
            from company
            WHERE ("CompanyType" = 'sale'
            OR "CompanyType" = 'sale-purchase')
            AND ("relyn" = '0' OR "relyn" IS NULL)
            and id = :id
			""";

		result.data = this.sqlRunner.getRows(sql, paramMap);

		return result;
	}

	@RequestMapping("/search_Bank")
	public AjaxResult getSearchBank(
			@RequestParam(value = "bankCode", required = false) String bankCode,
			@RequestParam(value = "bankName", required = false) String bankName){

		MapSqlParameterSource paramMap = new MapSqlParameterSource();

		AjaxResult result = new AjaxResult();

		String sql = """
            select bankid, banknm as bankname, bankpopcd as bankcode
			from tb_xbank
			where useyn = '1'
			""";

		if (bankCode != null && !bankCode.isEmpty()) {
			sql += " AND bankpopcd ILIKE :bankpopcd ";
			paramMap.addValue("bankpopcd", "%" + bankCode + "%");
		}

		if (bankName != null && !bankName.isEmpty()) {
			sql += " AND banknm ILIKE :bankName ";
			paramMap.addValue("bankName", "%" + bankName + "%");
		}

		result.data = this.sqlRunner.getRows(sql, paramMap);

		return result;
	}

	@GetMapping("/search_Account")
	public AjaxResult getSearchAccount(@RequestParam(value = "BankName", required = false) String bankName,
																		 @RequestParam(value = "accountNumber", required = false) String accountNumber) {
		AjaxResult result = new AjaxResult();
		MapSqlParameterSource paramMap = new MapSqlParameterSource();

		paramMap.addValue("bankName", bankName);
		paramMap.addValue("accountNumber", accountNumber);

		String sql = """
        SELECT
            ta.accid,
            tx.banknm AS "BankName",
            ta.bankid AS "bankId",
            ta.accnum AS "accountNumber", -- 암호화된 계좌번호
            ta.accname AS "accountName",
            CASE
                WHEN ta.popsort = '1' THEN '개인'
                WHEN ta.popsort = '0' THEN '법인'
            END AS "accountType"
        FROM tb_account ta
        LEFT JOIN tb_xbank tx ON ta.bankid = tx.bankid
        WHERE 1=1
    """;

		if (bankName != null && !bankName.isEmpty()) {
			sql += " AND tx.banknm ILIKE :bankName ";
			paramMap.addValue("bankName", "%" + bankName + "%");
		}

		// 쿼리는 전체 계좌 가져오고, 자바단에서 복호화 후 필터링
		List<Map<String, Object>> rawResults = this.sqlRunner.getRows(sql, paramMap);

		// 자바단에서 복호화 + accountNumber 포함 여부 확인
		List<Map<String, Object>> filtered = rawResults.stream()
				.filter(item -> {
					String encrypted = String.valueOf(item.get("accountNumber"));
          String decrypted = null; // 복호화 함수
          try {
            decrypted = decrypt(encrypted);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
          return decrypted.contains(accountNumber); // 부분 검색
				})
				.map(item -> {
					// 복호화된 값을 덮어쓰기 또는 별도 필드에 저장
          try {
            item.put("accountNumber", decrypt(item.get("accountNumber").toString()));
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
          return item;
				})
				.collect(Collectors.toList());

		result.data = filtered;
		return result;
	}

	@GetMapping("/search_AccountCode")
	public AjaxResult getsearch_AccountCode(@RequestParam(value = "acccd")String acccd,
																		 @RequestParam(value = "accnm") String accnm) {
		AjaxResult result = new AjaxResult();
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("acccd", acccd);
		paramMap.addValue("accnm", accnm);
		//log.info("계정코드 팝업 요청 들어옴 --- acccd:{}, accnm:{}", acccd,accnm);
		String sql = """
				select ta.acccd, ta.accnm
				from tb_accsubject ta 
				where ta.useyn ='Y' 
				""";

		if (acccd != null && !acccd.isEmpty()) {
			sql += " AND acccd ILIKE :acccd  ";
			paramMap.addValue("acccd", "%" + acccd + "%");
		}

		if (accnm != null && !accnm.isEmpty()) {
			sql += " AND ta.accnm ILIKE :accnm ";
			paramMap.addValue("accnm", "%" + accnm + "%");
		}

//		log.info(" 최종 SQL: {}", sql);
//		log.info(" 파라미터: {}", paramMap.getValues());
		result.data = this.sqlRunner.getRows(sql, paramMap);
		return result;
	}

	@RequestMapping("/search_Comp_purchase")
	public AjaxResult getSearchCompPurchase(
			@RequestParam(value = "compCode", required = false) String compCode,
			@RequestParam(value = "compName", required = false) String compName,
			@RequestParam(value = "business_number", required = false) String business_number){

		String tenantId = TenantContext.get();
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("compCode", compCode);
		paramMap.addValue("compName", compName);
		paramMap.addValue("business_number", business_number);
		paramMap.addValue("spjangcd", tenantId);
		AjaxResult result = new AjaxResult();

		String sql = """
            select id as id
            , "Name" as compName
            , "Code" as compCode
            , "BusinessNumber" as business_number
            , "TelNumber" as tel_number
            , "CEOName" as invoiceeceoname
            , "Address" as invoiceeaddr
            , "BusinessType" as invoiceebiztype
            , "BusinessItem" as invoiceebizclass
            , "AccountManager" as invoiceecontactname1
            , "AccountManagerPhone" as invoiceetel1
            , "Email" as invoiceeemail1
            from company
            WHERE ("CompanyType" = 'purchase'
            OR "CompanyType" = 'sale-purchase')
            AND ("relyn" = '0' OR "relyn" IS NULL)
            and spjangcd = :spjangcd
			""";
		//relyn = 거래중지 여부
		
		if (compCode != null && !compCode.isEmpty()) {
			sql += " AND \"Code\" ILIKE :compCode ";
			paramMap.addValue("compCode", "%" + compCode + "%");
		}

		if (compName != null && !compName.isEmpty()) {
			sql += " AND \"Name\" ILIKE :compName ";
			paramMap.addValue("compName", "%" + compName + "%");
		}

		if (business_number != null && !business_number.isEmpty()) {
			sql += " AND \"BusinessNumber\" ILIKE :business_number ";
			paramMap.addValue("business_number", "%" + business_number + "%");
		}

		sql += " ORDER BY \"Name\" ASC ";

		result.data = this.sqlRunner.getRows(sql, paramMap);

		return result;
	}

	@RequestMapping("/search_Comp_all")
	public AjaxResult getSearchCompAll(
			@RequestParam(value = "compCode", required = false) String compCode,
			@RequestParam(value = "compName", required = false) String compName,
			@RequestParam(value = "business_number", required = false) String business_number){

		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("compCode", compCode);
		paramMap.addValue("compName", compName);
		paramMap.addValue("business_number", business_number);
		AjaxResult result = new AjaxResult();

		String sql = """
            select id as id
            , "Name" as compName
            , "Code" as compCode
            , "BusinessNumber" as business_number
            , "TelNumber" as tel_number
            , "CEOName" as invoiceeceoname
            , "Address" as invoiceeaddr
            , "BusinessType" as invoiceebiztype
            , "BusinessItem" as invoiceebizclass
            , "AccountManager" as invoiceecontactname1
            , "AccountManagerPhone" as invoiceetel1
            , "Email" as invoiceeemail1
            from company
            WHERE ("relyn" = '0' OR "relyn" IS NULL)
			""";

		if (compCode != null && !compCode.isEmpty()) {
			sql += " AND \"Code\" ILIKE :compCode ";
			paramMap.addValue("compCode", "%" + compCode + "%");
		}

		if (compName != null && !compName.isEmpty()) {
			sql += " AND \"Name\" ILIKE :compName ";
			paramMap.addValue("compName", "%" + compName + "%");
		}

		if (business_number != null && !business_number.isEmpty()) {
			sql += " AND \"BusinessNumber\" ILIKE :business_number ";
			paramMap.addValue("business_number", "%" + business_number + "%");
		}

		sql += " ORDER BY \"Name\" ASC ";

		result.data = this.sqlRunner.getRows(sql, paramMap);

		return result;
	}

	}