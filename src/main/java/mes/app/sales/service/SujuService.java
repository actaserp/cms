package mes.app.sales.service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;

import mes.app.definition.service.material.UnitPriceService;
import mes.app.notification.BizEventTrigger;
import mes.config.Settings;
import mes.domain.entity.SujuHead;
import mes.domain.entity.User;
import mes.domain.repository.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.util.StringUtils;

import mes.domain.entity.Suju;
import mes.domain.services.CommonUtil;
import mes.domain.services.SqlRunner;

import javax.transaction.Transactional;

@Service
public class SujuService {

	@Autowired
	SqlRunner sqlRunner;
	
	@Autowired
	SujuRepository SujuRepository;

	@Autowired
	SujuUploadService sujuUploadService;

	@Autowired
	SujuHeadRepository sujuHeadRepository;

	@Autowired
	MaterialRepository materialRepository;

	@Autowired
	CompanyRepository companyRepository;

	@Autowired
	ProjectRepository projectRepository;

	@Autowired
	Settings settings;

	@Autowired
	DepartRepository departRepository;

	@Autowired
	UnitPriceService unitPriceService;

	@Autowired
	UnitRepository unitRepository;

	@Autowired
	private SujuRepository sujuRepository;
	
	
	// 수주 내역 조회 
	public List<Map<String, Object>> getSujuList(String date_kind, Timestamp start, Timestamp end, String spjangcd) {
		
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("date_kind", date_kind);
		dicParam.addValue("start", start);
		dicParam.addValue("end", end);
		dicParam.addValue("spjangcd", spjangcd);
		
		String sql = """
			WITH suju_state_summary AS (
			  SELECT
				sh.id AS suju_head_id,
				-- 상태 요약 계산
				CASE
				  WHEN COUNT(DISTINCT s."State") = 1 THEN MIN(s."State")
				  WHEN BOOL_AND(s."State" IN ('received', 'planned')) AND BOOL_OR(s."State" = 'planned') THEN 'part_planned'
				  WHEN BOOL_AND(s."State" IN ('received', 'ordered', 'planned')) AND BOOL_OR(s."State" = 'ordered') THEN 'part_ordered'
				  ELSE '기타'
				END AS summary_state
			   
			  FROM suju_head sh
			  JOIN suju s ON s."SujuHead_id" = sh.id
			   
			  GROUP BY sh.id
			),
			shipment_summary AS (
				SELECT
					s."SujuHead_id",
					SUM(s."SujuQty") AS total_qty,
					COALESCE(SUM(shp."shippedQty"), 0) AS total_shipped,
					CASE
					  WHEN COUNT(shp."shippedQty") = 0 THEN ''
					  WHEN SUM(shp."shippedQty") >= SUM(s."SujuQty") THEN 'shipped'
					  WHEN SUM(shp."shippedQty") < SUM(s."SujuQty") THEN 'partial'
					ELSE ''
					END AS shipment_state
				  FROM suju s
				  LEFT JOIN (
					SELECT "SourceDataPk", SUM("Qty") AS "shippedQty"
					FROM shipment
					GROUP BY "SourceDataPk"
				  ) shp ON shp."SourceDataPk" = s.id
				  GROUP BY s."SujuHead_id"
			)
			   
			SELECT
			  sh.id,
			  sh."JumunNumber",
			  to_char(sh."JumunDate", 'yyyy-mm-dd') AS "JumunDate",
			  to_char(sh."DeliveryDate", 'yyyy-mm-dd') AS "DueDate",
			  sh."Company_id",
			  c."Name" AS "CompanyName",
			  sh."TotalPrice",
			  sh."Description",
			  sc_state."Value" AS "StateName",
			  sc_type."Value" AS "SujuTypeName",
			   
			  -- 대표 제품명 + 외 N개
			  CASE
				WHEN COUNT(DISTINCT s."Material_id") = 1 THEN MAX(m."Name")
				ELSE CONCAT(MAX(m."Name"), ' 외 ', COUNT(DISTINCT s."Material_id") - 1, '개')
			  END AS product_name,
			   
			  sss.summary_state AS "State",
			  sc_ship."Value" AS "ShipmentStateName"
			   
			FROM suju_head sh
			JOIN suju s ON s."SujuHead_id" = sh.id
			JOIN material m ON m.id = s."Material_id"
			LEFT JOIN (
			  SELECT "SourceDataPk", SUM("Qty") AS "shippedQty"
			  FROM shipment
			  GROUP BY "SourceDataPk"
			) shp ON shp."SourceDataPk" = s.id
			LEFT JOIN company c ON c.id = sh."Company_id"
			LEFT JOIN shipment_summary ss ON ss."SujuHead_id" = sh.id
			LEFT JOIN suju_state_summary sss ON sss.suju_head_id = sh.id
			LEFT JOIN sys_code sc_state ON sc_state."Code" = sss.summary_state AND sc_state."CodeType" = 'suju_state'
			LEFT JOIN sys_code sc_type ON sc_type."Code" = sh."SujuType" AND sc_type."CodeType" = 'suju_type'
			LEFT JOIN sys_code sc_ship ON sc_ship."Code" = ss.shipment_state AND sc_ship."CodeType" = 'shipment_state'
            where 1 = 1
            and sh.spjangcd = :spjangcd
			""";

		if (date_kind.equals("sales")) {
			sql += """
        		and sh."JumunDate" between :start and :end
				group by
					 sh.id,
					 sh."JumunNumber",
					 sh."JumunDate",
					 sh."DeliveryDate",
					 sh."Company_id",
					 c."Name",
					 sh."TotalPrice",
					 sh."Description",
					 sh."SujuType",
					 sss.summary_state,
					 sc_state."Value",
					 sc_type."Value",
					 sc_ship."Value"
				order by sh."JumunDate" desc,  sh.id desc
			""";
		} else {
			sql += """
				and sh."DeliveryDate" between :start and :end
				group by
					 sh.id,
					 sh."JumunNumber",
					 sh."JumunDate",
					 sh."DeliveryDate",
					 sh."Company_id",
					 c."Name",
					 sh."TotalPrice",
					 sh."Description",
					 sh."SujuType",
					 sss.summary_state,
					 sc_state."Value",
					 sc_type."Value",
					 sc_ship."Value"
				order by sh."DeliveryDate" desc,  sh.id desc
			""";
		}


		List<Map<String, Object>> itmes = this.sqlRunner.getRows(sql, dicParam);
		
		return itmes;
	}
	
	// 수주 상세정보 조회
	public Map<String, Object> getSujuDetail(int id) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("id", id);

		String sql = """ 
			SELECT
				sh.id,
				sh."JumunNumber",
				to_char(sh."JumunDate", 'yyyy-mm-dd') AS "JumunDate",
				to_char(sh."DeliveryDate", 'yyyy-mm-dd') AS "DueDate",
				sh."Company_id",
				c."Name" AS "CompanyName",
				sh."TotalPrice",
				sh."Description",
				sh."SujuType",
				fn_code_name('suju_type', sh."SujuType") AS "SujuTypeName"
			FROM suju_head sh
			LEFT JOIN company c ON c.id = sh."Company_id"
			WHERE sh.id = :id
		""";

		String detailSql = """ 
			WITH shipment_status AS (
				     SELECT "SourceDataPk", SUM("Qty") AS shipped_qty
				     FROM shipment
				     WHERE "SourceTableName" = 'rela_data'
				     GROUP BY "SourceDataPk"
				 ),
				 suju_with_state AS (
				     SELECT
				         s.id AS suju_id,
				         s."SujuHead_id",
				         s."Material_id",
				         m."Code" AS "product_code",
				         m."Name" AS "txtProductName",
				         mg."Name" AS "MaterialGroupName",
				         mg.id AS "MaterialGroup_id",
				         u."Name" AS "unit",
				         s."SujuQty" AS "quantity",
				         to_char(s."JumunDate", 'yyyy-mm-dd') AS "JumunDate",
				         to_char(s."DueDate", 'yyyy-mm-dd') AS "DueDate",
				         s."CompanyName",
				         s."Company_id",
				         s."SujuType",
				         s."UnitPrice" AS "unitPrice",
				         s."Vat" AS "VatAmount",
				         s."Price" AS "supplyAmount",
				         s."TotalAmount" AS "totalAmount",
				         to_char(s."_created", 'yyyy-mm-dd') AS "create_date",
				         s.project_id AS "projectHidden",
				         p.projnm AS "project",
				         s."Description" AS "description",
				         s."InVatYN" AS "invatyn",
				         s."SujuQty2",
				         s."AvailableStock",
				         s."ReservationStock",
				         s."State" AS "original_state",
				         COALESCE(sh.shipped_qty, -1) AS "shipped_qty",
				         
				         CASE
				             WHEN sh.shipped_qty = -1 THEN s."State"
				             WHEN sh.shipped_qty = 0 THEN 'force_complement'
				             WHEN sh.shipped_qty >= s."SujuQty" THEN 'shipped'
				             WHEN sh.shipped_qty < s."SujuQty" THEN 'partial'
				             ELSE s."State"
				         END AS final_state
				 
				     FROM suju s
				     INNER JOIN material m ON m.id = s."Material_id"
				     INNER JOIN mat_grp mg ON mg.id = m."MaterialGroup_id"
				     LEFT JOIN unit u ON m."Unit_id" = u.id
				     LEFT JOIN TB_DA003 p ON p."projno" = s.project_id
				     LEFT JOIN shipment_status sh ON sh."SourceDataPk" = s.id
				     WHERE s."SujuHead_id" = :id
				 )
				 
				 SELECT
				     s."suju_id",
				     s."SujuHead_id",
				     s."Material_id",
				     s."product_code",
				     s."txtProductName",
				     s."MaterialGroupName",
				     s."MaterialGroup_id",
				     s."unit",
				     s."quantity",
				     s."JumunDate",
				     s."DueDate",
				     s."CompanyName",
				     s."Company_id",
				     s."SujuType",
				     s."unitPrice",
				     s."VatAmount",
				     s."supplyAmount",
				     s."totalAmount",
				     s.final_state AS "State",
				     
				     COALESCE(sc_ship."Value", sc_suju."Value") AS "suju_StateName",
				 
				     s."invatyn",
				     s."SujuQty2",
				     s."AvailableStock",
				     s."ReservationStock",
				     s."create_date",
				     s."projectHidden",
				     s."project",
				     s."description"
				 
				 FROM suju_with_state s
				 LEFT JOIN sys_code sc_ship
				     ON sc_ship."Code" = s.final_state AND sc_ship."CodeType" = 'shipment_state'
				 LEFT JOIN sys_code sc_suju
				     ON sc_suju."Code" = s.final_state AND sc_suju."CodeType" = 'suju_state'
				 ORDER BY s."JumunDate";
				 
		""";

		Map<String, Object> sujuHead = this.sqlRunner.getRow(sql, paramMap);
		List<Map<String, Object>> sujuList = this.sqlRunner.getRows(detailSql, paramMap);

		sujuHead.put("sujuList", sujuList);
		
		return sujuHead;
	}

	@Transactional
	@BizEventTrigger(domain = "wm_suju_list_a", action = "SAVE")
	public Integer saveManual(Map<String, Object> payload, Authentication auth) {

		User user = (User) auth.getPrincipal();

		String jumunDateStr = (String) payload.get("JumunDate");
		String dueDateStr = (String) payload.get("DueDate");

		Date jumunDate = CommonUtil.trySqlDate(jumunDateStr);
		Date dueDate = CommonUtil.trySqlDate(dueDateStr);

		String companyName = (String) payload.get("CompanyName");
		Integer companyId = Integer.parseInt(payload.get("Company_id").toString());
		String sujuType = (String) payload.get("SujuType");
		String description = (String) payload.get("Description");
		String projectId = (String) payload.get("projectHidden");
		String spjangcd = (String) payload.get("spjangcd");
		String amountStr = payload.get("totalAmountSum").toString().replace(",", "");
		double totalAmount = 0.0;
		try {
			if (amountStr != null && !amountStr.trim().isEmpty()) {
				totalAmount = Double.parseDouble(amountStr.trim().replace(",", ""));
			}
		} catch (NumberFormatException e) {
			// 무시하고 0 유지
		}
		List<Map<String, Object>> items = (List<Map<String, Object>>) payload.get("items");

		SujuHead head;

		// ✅ suju_head 수정 여부 확인
		if (payload.containsKey("id") && payload.get("id") != null && !payload.get("id").toString().isEmpty()) {
			Integer headId = Integer.parseInt(payload.get("id").toString());
			head = sujuHeadRepository.findById(headId).orElse(new SujuHead());
		} else {
			head = new SujuHead();
			head.setJumunNumber(generateJumunNumber(jumunDate));
		}

		head.setJumunDate(jumunDate);
		head.setDeliveryDate(dueDate);
		head.setCompany_id(companyId);
		head.setSpjangcd(spjangcd);
		head.set_audit(user);
		head.setSujuType(sujuType);
		head.setTotalPrice(totalAmount);
		head.setDescription(description);
		head.set_status("manual");
		head = sujuHeadRepository.save(head);

		for (Map<String, Object> item : items) {
			Suju suju;

			// ✅ 수정인지 확인
			if (item.containsKey("suju_id") && item.get("suju_id") != null && !item.get("suju_id").toString().isEmpty()) {
				Integer sujuId = Integer.parseInt(item.get("suju_id").toString());
				suju = SujuRepository.findById(sujuId).orElse(new Suju());
			} else {
				suju = new Suju(); // 신규일 경우
				suju.setJumunNumber(head.getJumunNumber());
			}

			// 공통 필드 설정
			suju.setSujuHeadId(head.getId());
			suju.setJumunDate(jumunDate);
			suju.setDueDate(dueDate);
			suju.setCompanyId(companyId);
			suju.setCompanyName(companyName);
			suju.setSpjangcd(spjangcd);
			suju.setProject_id(projectId);
			suju.set_status("manual");
			suju.setState("received");
			suju.set_audit(user);

			String invatyn = item.get("VatIncluded").toString();

			suju.setMaterialId(Integer.parseInt(item.get("Material_id").toString()));
			suju.setSujuQty(Integer.parseInt(item.get("quantity").toString()));
			suju.setSujuQty2(Integer.parseInt(item.get("quantity").toString()));
			suju.setUnitPrice(Double.parseDouble(item.get("unitPrice").toString()));
			suju.setPrice(Double.parseDouble(item.get("supplyAmount").toString()));
			suju.setVat(Double.parseDouble(item.get("VatAmount").toString()));
			suju.setTotalAmount(Double.parseDouble(item.get("totalAmount").toString()));
			suju.setProject_id(item.get("projectHidden").toString());
			suju.setInVatYN(invatyn);
			suju.setDescription((String) item.get("description"));
			suju.setConfirm("0");

			// 단가 변경 시 처리
			Boolean unitPriceChanged = (Boolean) item.get("unitPriceChanged");
			if (unitPriceChanged != null && unitPriceChanged) {
				MultiValueMap<String, Object> priceData = new LinkedMultiValueMap<>();
				priceData.add("Material_id", suju.getMaterialId());
				priceData.add("Company_id", companyId);
				priceData.add("UnitPrices", suju.getUnitPrice());
				priceData.add("ApplyStartDate", LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
				priceData.add("type", "02");
				priceData.add("ChangerName", user.getUsername());
				priceData.add("user_id", user.getId());

				unitPriceService.saveCompanyUnitPrice(priceData);
			}

			SujuRepository.save(suju);
		}

		return head.getId();
    }

	public String generateJumunNumber(Date jumunDate) {
		String dateStr = new SimpleDateFormat("yyyyMMdd").format(jumunDate);

		String sql = """
			WITH upsert AS (
				INSERT INTO seq_maker ("Code", "BaseDate", "CurrVal", "_modified")
				SELECT 'JumunNumber', '20250626', 1, now()
				WHERE NOT EXISTS (
					SELECT 1 FROM seq_maker WHERE "Code" = 'JumunNumber' AND "BaseDate" = '20250626'
				)
				RETURNING "CurrVal"
			),
			updated AS (
				UPDATE seq_maker
				SET "CurrVal" = "CurrVal" + 1, "_modified" = now()
				WHERE "Code" = 'JumunNumber' AND "BaseDate" = '20250626'
				RETURNING "CurrVal"
			)
			SELECT * FROM updated
			UNION ALL
			SELECT * FROM upsert;
        """;

		MapSqlParameterSource param = new MapSqlParameterSource();
		param.addValue("date", dateStr);

		Integer nextVal = this.sqlRunner.queryForObject(sql, param, (rs, rowNum) -> rs.getInt(1) );
		return dateStr + "-" + String.format("%04d", nextVal);
	}
	
	// 제품 정보 조회
	public Map<String, Object> getSujuMatInfo(int product_id) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("product_id", product_id);
		
		String sql = """
			select m.id as mat_pk
			, m."AvailableStock" 
			, u."Name" as unit_name
			from material m 
			inner join unit u on u.id = m."Unit_id" 
			where m.id = :product_id
			""";
		
		Map<String, Object> item = this.sqlRunner.getRow(sql, paramMap);
		
		return item;
	}
	
	public String makeJumunNumber(Date dataDate) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("data_date", dataDate);
		
		String jumunNumber = "";
		
		String sql = """
		select "CurrVal" from seq_maker where "Code" = 'JumunNumber' and "BaseDate" = :data_date
		""";
		Map<String, Object> mapRow = this.sqlRunner.getRow(sql, paramMap);
		
		int currVal = 1;
		if (mapRow!=null && mapRow.containsKey("CurrVal")) {
			currVal =  (int)mapRow.get("CurrVal");
			sql = """
		    update seq_maker set "CurrVal" = "CurrVal" + 1, "_modified" = now()	where "Code" = 'JumunNumber' and "BaseDate" = :data_date
			""";
			this.sqlRunner.execute(sql, paramMap);
		}else {
			sql = """
			insert into seq_maker("Code", "BaseDate", "CurrVal", "_modified") values('JumunNumber', :data_date, 1, now());	
			""";
			this.sqlRunner.execute(sql, paramMap);
		}
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		jumunNumber = String.format("{0}-{1}", sdf.format(dataDate), currVal);
		return jumunNumber;	
	}
	
	public String makeJumunNumberAndUpdateSuju(int suju_id, String dataDate) {

		Suju suju = this.SujuRepository.getSujuById(suju_id);
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("data_date", dataDate);
		
		String jumunNumber = suju.getJumunNumber();
		if(StringUtils.hasText(jumunNumber)==false) {
			Date jumun_date = CommonUtil.trySqlDate(dataDate);
			jumunNumber = this.makeJumunNumber(jumun_date);
			suju.setJumunNumber(jumunNumber);
			this.SujuRepository.save(suju);
		}
		return jumunNumber;
	}

	public List<Map<String, Object>> getPriceByMatAndComp(int matPk, int company_id, String ApplyStartDate){
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("mat_pk", matPk);
		dicParam.addValue("company_id", company_id);
		dicParam.addValue("ApplyStartDate", ApplyStartDate);

		String sql = """
			select mcu.id 
            , mcu."Company_id"
            , c."Name" as "CompanyName"
            , mcu."UnitPrice" 
            , mcu."FormerUnitPrice" 
            , mcu."ApplyStartDate"::date 
            , mcu."ApplyEndDate"::date 
            , mcu."ChangeDate"::date 
            , mcu."ChangerName" 
            from mat_comp_uprice mcu 
            inner join company c on c.id = mcu."Company_id"
            where 1=1
            and mcu."Material_id" = :mat_pk
            and mcu."Company_id" = :company_id
            and to_date(:ApplyStartDate, 'YYYY-MM-DD') between mcu."ApplyStartDate"::date and mcu."ApplyEndDate"::date
            and mcu."Type" = '02'
            order by c."Name", mcu."ApplyStartDate" desc
        """;


		List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);
		return items;
	}

}
