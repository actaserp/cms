package mes.app.sales;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletRequest;
import javax.transaction.Transactional;

import lombok.extern.slf4j.Slf4j;
import mes.app.definition.service.material.UnitPriceService;
import mes.app.sales.service.SujuUploadService;
import mes.config.Settings;
import mes.domain.entity.*;
import mes.domain.repository.*;
import mes.domain.services.SqlRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.security.core.Authentication;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;

import mes.app.sales.service.SujuService;
import mes.domain.model.AjaxResult;
import mes.domain.services.CommonUtil;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.multipart.MultipartHttpServletRequest;

@Slf4j
@RestController
@RequestMapping("/api/sales/suju")
public class SujuController {

	@Autowired
	SujuRepository SujuRepository;
	
	@Autowired
	SujuService sujuService;

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
	SqlRunner sqlRunner;

	@Autowired
	DepartRepository departRepository;

	@Autowired
	UnitPriceService unitPriceService;

	@Autowired
	UnitRepository unitRepository;
    @Autowired
    private SujuRepository sujuRepository;

	// 수주 목록 조회 
	@GetMapping("/read")
	public AjaxResult getSujuList(
			@RequestParam(value="date_kind", required=false) String date_kind,
			@RequestParam(value="start", required=false) String start_date,
			@RequestParam(value="end", required=false) String end_date,
			@RequestParam(value="spjangcd") String spjangcd,
			HttpServletRequest request) {
		
		start_date = start_date + " 00:00:00";
		end_date = end_date + " 23:59:59";
		
		Timestamp start = Timestamp.valueOf(start_date);
		Timestamp end = Timestamp.valueOf(end_date);
		
		List<Map<String, Object>> items = this.sujuService.getSujuList(date_kind, start, end, spjangcd);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	
	// 수주 상세정보 조회
	@GetMapping("/detail")
	public AjaxResult getSujuDetail(
			@RequestParam("id") int id,
			HttpServletRequest request) {
		Map<String, Object> item = this.sujuService.getSujuDetail(id);
		
		AjaxResult result = new AjaxResult();
		result.data = item;
		
		return result;
	}
	
	// 제품 정보 조회
	@GetMapping("/product_info")
	public AjaxResult getSujuMatInfo(
			@RequestParam("product_id") int id,
			HttpServletRequest request) {
		Map<String, Object> item = this.sujuService.getSujuMatInfo(id);
		
		AjaxResult result = new AjaxResult();
		result.data = item;
		
		return result;
	}
	
	// 수주 등록 
	@PostMapping("/manual_save")
	public AjaxResult SujuSave(@RequestBody Map<String, Object> payload, Authentication auth) {

		Integer sujuHeadId = sujuService.saveManual(payload, auth);

		AjaxResult result = new AjaxResult();
		result.success = true;
		result.data = sujuHeadId;
		return result;
	}

	// 수주 삭제
	@Transactional
	@PostMapping("/delete")
	public AjaxResult deleteSuju(
			@RequestParam("id") Integer id,
			@RequestParam("State") String State,
			@RequestParam("ShipmentStateName") String ShipmentStateName) {
		
		AjaxResult result = new AjaxResult();
		
		if (State.equals("received")==false) {
			//received 아닌것만
			result.success = false;
			result.message = "수주상태만 삭제할 수 있습니다";
			return result;
		}
		if (ShipmentStateName != null && !ShipmentStateName.isEmpty()) {
			result.success = false;
			result.message = "출하된 수주는 삭제할 수 없습니다";
			return result;
		}

		SujuRepository.deleteBySujuHeadId(id);
		sujuHeadRepository.deleteById(id);
		
		return result;
	}

	// 단가 정보 가져오기
	@GetMapping("/readPriceSuju")
	public AjaxResult getPriceHistory(@RequestParam("mat_pk") int matPk,
									  @RequestParam("company_id") int company_id,
									  @RequestParam("JumunDate") String ApplyStartDate) {

		List<Map<String, Object>> items = this.sujuService.getPriceByMatAndComp(matPk, company_id, ApplyStartDate);

		AjaxResult result = new AjaxResult();
		result.data = items;

		return result;
	}

	/**
	 * 엑셀 컬럼 순서
	 업체명 - 사업부 - 프로젝트 - 발주번호 - 자재번호 - 품명 - 규격 - 수량 - 단가 - 금액 - 단위 - 발주일 - 요청일
	 #company_name_col = 0		# 업체명
	 #depart_name_col = 1		# 사업부 이름 - 부서로 등록함
	 #project_name_col = 2		# 프로젝트 이름
	 #jumun_number_col = 3		# 발주 받은 번호(수주 번호)
	 #prod_code_col = 4			# 자재 번호(품목 코드)
	 #prod_name_col = 5    		# 품명
	 #prod_standard1_col = 6	# 규격
	 #qty_col = 7				# 수량
	 #prod_unit_price_col = 8	# 단가
	 #total_price_col = 9		# 금액
	 #unit_name_col = 10		# 단위
	 #jumnun_date_col = 11		# 발주일
	 #due_date_col = 12			# 요청일


	 **/
	// 수주 엑셀 업로드
	@Transactional
	@PostMapping("/upload_save")
	public AjaxResult saveSujuBulkData(
			@RequestParam(value="data_date") String data_date,
			@RequestParam(value="spjangcd") String spjangcd,
			@RequestParam(value="upload_file") MultipartFile upload_file,
			MultipartHttpServletRequest multipartRequest,
			Authentication auth) throws FileNotFoundException, IOException {

		User user = (User)auth.getPrincipal();

//	 	int company_name_col = 0;
		int depart_name_col = 2;
	 	int project_name_col = 4;
	 	int jumun_number_col = 5;
	 	int prod_code_col = 7;
	 	int prod_name_col = 8;
		int prod_standard1_col = 9;
		int qty_col = 10;
		int prod_unit_price_col = 12;
		int total_price_col = 13;
		int unit_name_col = 14;
		int jumnun_date_col = 15;
		int due_date_col = 16;

		DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
		LocalDateTime now = LocalDateTime.now();
		String formattedDate = dtf.format(now);
		String upload_filename = settings.getProperty("file_temp_upload_path") + formattedDate + "_" + upload_file.getOriginalFilename();


		if (new File(upload_filename).exists()) {
			new File(upload_filename).delete();
		}

		try (FileOutputStream destination = new FileOutputStream(upload_filename)) {
			destination.write(upload_file.getBytes());
		}

		List<List<String>> suju_file = this.sujuUploadService.excel_read(upload_filename);
		List<Company> CompanyList = companyRepository.findBySpjangcd(spjangcd);
		List<TB_DA003> projectList = projectRepository.findByIdSpjangcd(spjangcd);
		List<Material> materialList = materialRepository.findBySpjangcd(spjangcd);
		List<Depart> departList = departRepository.findBySpjangcd(spjangcd);
		List<Unit> unitList = unitRepository.findAll();
		Map<String, SujuHead> sujuHeadMap = new HashMap<>();

		List<Suju> sujuList = new ArrayList<>();

		Map<String, Company> companyMap = CompanyList.stream()
				.collect(Collectors.toMap(Company::getName, Function.identity()));

		Map<String, Depart> departMap = departList.stream()
				.collect(Collectors.toMap(Depart::getName, Function.identity()));

		Map<String, TB_DA003> projectMap = projectList.stream()
				.collect(Collectors.toMap(TB_DA003::getProjnm, Function.identity()));

		Map<String, Material> materialMap = materialList.stream()
				.filter(m -> m.getCustomerBarcode() != null && !m.getCustomerBarcode().trim().isEmpty())
				.collect(Collectors.toMap(
						Material::getCustomerBarcode,
						Function.identity(),
						(existing, duplicate) -> existing
				));

		Map<String, Unit> unitMap = unitList.stream()
				.collect(Collectors.toMap(Unit::getName, Function.identity()));


		AjaxResult result = new AjaxResult();

		for (int i=0; i < suju_file.size(); i++) {

			List<String> row = suju_file.get(i);

//			String company_name = row.get(company_name_col).trim();
			String company_name = "대양전기공업㈜";
			String depart_name = row.get(depart_name_col).trim();
			String rawProjectName = row.get(project_name_col).trim();
			String project_name = rawProjectName.split("\\s+")[0];
			String jumun_number = row.get(jumun_number_col).trim();
			String prod_code_raw = row.get(prod_code_col);
			String prod_code;

			try {
				// Excel에서 숫자로 인식된 경우 (Double 타입)
				double doubleValue = Double.parseDouble(prod_code_raw);
				prod_code = new BigDecimal(doubleValue).toPlainString();  // 소수점 없는 문자열로 변환
			} catch (NumberFormatException e) {
				// 애초에 문자열로 잘 들어온 경우
				prod_code = prod_code_raw.trim();
			}
			String prod_name = row.get(prod_name_col).trim();
			String prod_standard = row.get(prod_standard1_col).trim();
			Float floatQty = Float.parseFloat(row.get(qty_col).trim());
			Integer quantity = floatQty.intValue();
			Float unit_price = tryFloat(row.get(prod_unit_price_col));
			Float total_price = tryFloat(row.get(total_price_col));
			String raw = row.get(total_price_col);
			String unit_name = row.get(unit_name_col).trim();

			LocalDate jumun_date = parseFlexibleDate(row.get(jumnun_date_col).trim());
			LocalDate due_date = parseFlexibleDate(row.get(due_date_col).trim());

			Company company = companyMap.get(company_name);

			if (company == null) {
				result.message = "엑셀에 기입된 업체명 '" + company_name + "'이(가) 존재하지 않습니다.";
				result.success = false;
				return result;
			}

			// 부서 확인 또는 생성
			Depart depart = departMap.get(depart_name);

			if (depart == null) {
				depart = new Depart();
				depart.setName(depart_name);
				depart.setSpjangcd(spjangcd);
				depart.set_audit(user);
				depart = departRepository.save(depart);
				departMap.put(depart_name, depart);
			}

			// 단위 확인 또는 생성
			Unit unit = unitMap.get(unit_name);

			if (unit == null) {
				unit = new Unit();
				unit.setName(depart_name);
				unit.setSpjangcd(spjangcd);
				unit = unitRepository.save(unit);
				unitMap.put(unit_name, unit);
			}

			// Project 확인 또는 생성
			TB_DA003 project = projectMap.get(project_name);

			if (project == null) {
				project = new TB_DA003();
				String newProjNo = generateNewProjectNo();
				project.setId(new TB_DA003Id(spjangcd, newProjNo));
				project.setProjnm(project_name);
				project.setBalcltcd(company.getId());
				project.setBalcltnm(company_name);
				project = projectRepository.save(project);
				projectMap.put(project_name, project);
			}

			// Material 매칭
			Material material = materialMap.get(prod_code);
			LocalDateTime jumunDateTime = LocalDateTime.of(jumun_date, LocalTime.now());
			DateTimeFormatter formatter1 = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
			String jumunDateTimeStr = jumunDateTime.format(formatter1);

			if (material == null) {
				material = new Material();
				material.setName(prod_name);
				material.setCode(prod_code);
				material.setFactory_id(1);
				material.setCustomerBarcode(prod_code);
				material.setStandard1(prod_standard);
				material.setMtyn("1");
				material.setUseyn("0");
				material.setMaterialGroupId(45);
				material.setSpjangcd(spjangcd);
				material.set_audit(user);
				material = materialRepository.save(material);
			} else {
				List<Map<String, Object>> items = this.sujuService.getPriceByMatAndComp(material.getId(), company.getId(), jumunDateTimeStr);

				Float oldUnitPrice = items.isEmpty() ? null : ((Number) items.get(0).get("UnitPrice")).floatValue();
				boolean unitPriceChanged = (oldUnitPrice == null || !Objects.equals(oldUnitPrice.intValue(), unit_price.intValue()));

				if (unitPriceChanged) {
					String hhmmss = LocalTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss"));
					String applyStartDate = jumun_date + "T" + hhmmss;

					MultiValueMap<String, Object> priceData = new LinkedMultiValueMap<>();
					priceData.set("Material_id", material.getId());
					priceData.set("Company_id", company.getId());
					priceData.set("UnitPrice", unit_price.intValue()); // DB 정수형이면
					priceData.set("ApplyStartDate", applyStartDate);
					priceData.set("type", "02");
					priceData.set("user_id", user.getId());

					unitPriceService.saveCompanyUnitPrice(priceData);
				}

			}

			SujuHead sujuHead = sujuHeadMap.get(jumun_number);

			if (sujuHead == null) {
				sujuHead = sujuHeadRepository.findByJumunNumberAndSpjangcd(jumun_number, spjangcd)
						.orElseGet(() -> {
							SujuHead newHead = new SujuHead();
							newHead.setCompany_id(company.getId());   // 해당 행 기준
							newHead.setJumunDate(Date.valueOf(jumun_date));
							newHead.setDeliveryDate(Date.valueOf(due_date));
							newHead.setSpjangcd(spjangcd);
							newHead.setJumunNumber(jumun_number);
							newHead.set_audit(user);
							newHead.set_audit(user);
							newHead.setSujuType("sales");
							return sujuHeadRepository.save(newHead);
						});

				sujuHeadMap.put(jumun_number, sujuHead);  // 캐싱
			}

			Suju suju = new Suju();
			suju.setState("received");

			suju.setSujuQty(quantity);
			suju.setSujuQty2(quantity);
			suju.setCompanyId(company.getId());
			suju.setCompanyName(company_name);
			suju.setDueDate(Date.valueOf(due_date));
			suju.setJumunDate(Date.valueOf(jumun_date));
			suju.setJumunNumber(jumun_number);
			suju.setMaterialId(material.getId());
			suju.setAvailableStock((float) 0); // 없으면 0으로 보내기 추가
			suju.set_status("manual");
			suju.set_audit(user);
			suju.setUnitPrice(unit_price.doubleValue());
			suju.setPrice(total_price.doubleValue());
			suju.setVat(total_price.doubleValue() * 0.1);
			suju.setTotalAmount(total_price.doubleValue() + (total_price.doubleValue() * 0.1));
			suju.setInVatYN("N");
			suju.setProject_id(project.getId().getProjno());
			suju.setSpjangcd(spjangcd);
			suju.setConfirm("0");
			suju.setSujuHeadId(sujuHead.getId());
			sujuList.add(suju);

			try {
			} catch (Exception e) {
				log.error("Insert 실패 - row {}: {}", i, e.getMessage());
				continue;
			}

		}

		SujuRepository.saveAll(sujuList);


		result.success=true;
		return result;
	}

	private String generateNewProjectNo() {
		String year = String.valueOf(LocalDate.now().getYear());

		String maxProjNo = projectRepository.findMaxProjnoByYearPrefix(year + "-"); // ex: 2025-003

		int nextSeq = 1;
		if (maxProjNo != null && maxProjNo.length() >= 8) {
			String[] parts = maxProjNo.split("-");
			if (parts.length == 2) {
				try {
					nextSeq = Integer.parseInt(parts[1]) + 1;
				} catch (NumberFormatException ignored) {}
			}
		}

		return String.format("%s-%03d", year, nextSeq); // ex: 2025-004
	}

	public static LocalDate parseFlexibleDate(String value) {
		try {
			if (value.matches(".*[Ee].*")) {
				double d = Double.parseDouble(value);
				int intVal = (int) d;
				return LocalDate.parse(String.valueOf(intVal), DateTimeFormatter.ofPattern("yyyyMMdd"));
			} else if (value.matches("^\\d{8}$")) {
				return LocalDate.parse(value, DateTimeFormatter.ofPattern("yyyyMMdd"));
			} else {
				return LocalDate.parse(value, DateTimeFormatter.ofPattern("yyyy-MM-dd"));
			}
		} catch (Exception e) {
			throw new IllegalArgumentException("날짜 형식이 올바르지 않습니다: " + value);
		}
	}

	public static float tryFloat(String data) {
		if (!StringUtils.hasText(data)) {
			return 0;
		}

		try {
			// 숫자, 점, 음수만 남기고 나머지 제거
			data = data.replaceAll("[^0-9.\\-]", "");
			return Float.parseFloat(data);
		} catch (Exception e) {
			System.out.println("tryFloat: failed to parse [" + data + "]");
			return 0;
		}
	}



	// 수주 변환 changeSujuBulkData
	@PostMapping("/change")
	public AjaxResult changeSujuBulkData(
			@RequestParam MultiValueMap<String,Object> Q,
			HttpServletRequest request,
			Authentication auth) {

		AjaxResult result = new AjaxResult();

		User user = (User)auth.getPrincipal();

		List<Map<String, Object>> error_items = new ArrayList<>();
		String sql = "";

		List<Map<String, Object>> qItems = CommonUtil.loadJsonListMap(Q.getFirst("Q").toString());

		if (qItems.size() == 0) {
			result.success = false;
			return result;
		}

		for(int i = 0; i < qItems.size(); i++) {
			Integer id = Integer.parseInt(qItems.get(i).get("id").toString());
			String state = CommonUtil.tryString(qItems.get(i).get("state"));

			MapSqlParameterSource paramMap = new MapSqlParameterSource();
			paramMap.addValue("id", id);
			paramMap.addValue("user_pk", user.getId());

			//sujuUploadService.BeforeCheck();

			if (state.equals("엑셀")) {
				sql = """
					with A as (
                    select "JumunNumber", m.id as mat_pk, b."Quantity", b."JumunDate"::date, b."DueDate"::date, c.id as comp_pk, b."CompanyName"
                    , m."UnitPrice", case when m."VatExemptionYN" = 'Y' then 0 else 0.1 end as vat_pro
                    from suju_bulk b 
                    inner join material m on m."Code" = b."ProductCode"
                    --left join company c on c."Name" = b."CompanyName"
                    left join company c on c."Code"  = b."CompCode"
                    where b.id = :id
                ), B as (
                    select A.mat_pk, A.comp_pk, mcu."UnitPrice"
                    , row_number() over (partition by A.mat_pk, A.comp_pk order by mcu."ApplyStartDate" desc) as g_idx
                    from mat_comp_uprice mcu
                    inner join A on A.mat_pk = mcu."Material_id"
                    and A.comp_pk = mcu."Company_id"
                    and A."JumunDate" between mcu."ApplyStartDate" and mcu."ApplyEndDate"
                )
                insert into suju("JumunNumber", "Material_id", "SujuQty", "SujuQty2", "JumunDate", "DueDate", "Company_id", "CompanyName"
                , "UnitPrice", "Price", "Vat", "State", _status, _created, _creater_id )
                select A."JumunNumber", A.mat_pk, A."Quantity", A."Quantity", A."JumunDate", A."DueDate", A.comp_pk, A."CompanyName"
                , coalesce(B."UnitPrice", A."UnitPrice") as unit_price
                , coalesce(B."UnitPrice", A."UnitPrice") * a."Quantity" as price
                , A.vat_pro * coalesce(B."UnitPrice", A."UnitPrice") * a."Quantity" as vat
                , 'received', 'excel', now(), :user_pk
                from A 
                left join B on B.mat_pk = a.mat_pk
                and B.comp_pk = A.comp_pk 
                and B.g_idx = 1
				  """;
				this.sqlRunner.execute(sql, paramMap);

				sql = """
					update suju_bulk set _status = 'Suju' where id = :id
					  """;

				this.sqlRunner.execute(sql, paramMap);

			} else {
				Map<String, Object> err_item = new HashMap<>();
				err_item.put("success", false);
				//err_item.put("message", "Excel상태만 전환할 수 있습니다.");
				err_item.put("id", id);
				error_items.add(err_item);
			}

		}

		result.success=true;

		if( error_items.size() > 0 ) {
			result.success=false;
			result.message="엑셀 상태만 전환할 수 있습니다.";
		}

//		Map<String, Object> item = new HashMap<String, Object>();
//		item.put("error_items", error_items);
//
//		result.data=item;
		return result;
	}

	@Transactional
	@PostMapping("/force-complete")
	public AjaxResult forceCompleteSuju(@RequestBody Map<String, Object> payload) {
		AjaxResult result = new AjaxResult();

		List<Integer> sujuPkList = (List<Integer>) payload.get("sujuPkList");
		sujuRepository.forceCompleteSujuList(sujuPkList);
		return result;
	}



}
