package mes.app.definition;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;
import mes.app.definition.service.BomUploadService;
import mes.app.definition.service.material.UnitPriceService;
import mes.app.sales.service.SujuUploadService;
import mes.config.Settings;
import mes.domain.entity.*;
import mes.domain.repository.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.security.core.Authentication;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.definition.service.BomService;
import mes.domain.model.AjaxResult;
import mes.domain.services.SqlRunner;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.multipart.MultipartHttpServletRequest;

import javax.transaction.Transactional;

@Slf4j
@RestController
@RequestMapping("/api/definition/bom")
public class BomController {
	
	@Autowired
	SqlRunner sqlRunner;	
	
	@Autowired
	BomService bomService;

	@Autowired
	Settings settings;

	@Autowired
	BomUploadService bomUploadService;

	@Autowired
	CompanyRepository companyRepository;

	@Autowired
	ProjectRepository projectRepository;

	@Autowired
	MaterialRepository materialRepository;

	@Autowired
	DepartRepository departRepository;

	@Autowired
	UnitRepository unitRepository;

	@Autowired
	UnitPriceService unitPriceService;

    @Autowired
    private BomRepository bomRepository;

    @Autowired
    private BomComponentRepository bomComponentRepository;

	@RequestMapping("/read")
	public AjaxResult getMaterialList(
			@RequestParam(value="mat_type", required=false) String mat_type,
			@RequestParam(value="mat_group", required=false) Integer mat_group,
			@RequestParam(value="bom_type", required=false) String bom_type,
			@RequestParam(value="mat_name", required=false) String mat_name,
			@RequestParam(value="not_past_flag", required=false) String not_past_flag,
			@RequestParam(value ="spjangcd") String spjangcd
			) {
		
		AjaxResult result = new AjaxResult();  
        result.data = this.bomService.getBomMaterialList(mat_type,mat_group,bom_type, mat_name, not_past_flag,spjangcd);
		return result;
	}	
		

	
	@PostMapping("/save")
	public AjaxResult saveBom(
			@RequestParam(value="id", required = false) Integer id,
			@RequestParam(value="Name") String name,
			@RequestParam(value="Material_id") int materialId,
			@RequestParam(value="StartDate") String startDate,
			@RequestParam(value="EndDate") String endDate,
			@RequestParam(value="BOMType") String bomType,
			@RequestParam(value="Version") String version,
			@RequestParam(value="OutputAmount") float outputAmount,
			@RequestParam(value ="spjangcd") String spjangcd,
			Authentication auth	
			) {				
		
		User user = (User)auth.getPrincipal();
		
		AjaxResult result = new AjaxResult();
		
		startDate = startDate + " 00:00:00";
		endDate = endDate + " 23:59:59";
		
		Timestamp startTs = Timestamp.valueOf(startDate);
		Timestamp endTs = Timestamp.valueOf(endDate);
		
		boolean isSameVersion = this.bomService.checkSameVersion(id, materialId, bomType, version);
		
		if (isSameVersion==true) {
			result.success = false;
			result.message="중복된 BOM버전이 존재합니다.";
			return result;
		}
		
		boolean isDuplicated = this.bomService.checkDuplicatePeriod(id, materialId, bomType, startDate, endDate);
		if (isDuplicated) {
			result.success = false;
			result.message="기간이 겹치는 동일 제품의 \\n BOM이 존재합니다.";
			return result;			
		}
		
		Bom bom = null;
		if (id!=null) {
			bom = this.bomService.getBom(id);
		}else {
			bom = new Bom();
			if (StringUtils.hasText(version)==false) {
				version = "1.0";
			}
		}		
		
		bom.setName(name);
		bom.setMaterialId(materialId);
		bom.setOutputAmount(outputAmount);
		bom.setBomType(bomType);
		bom.setVersion(version);
		bom.setStartDate(startTs);
		bom.setEndDate(endTs);
		bom.set_audit(user);
		bom.setSpjangcd(spjangcd);


		this.bomService.saveBom(bom);		
		result.data = bom.getId();
		
		return result;
		
	}	
	
	@RequestMapping("/detail")
	public AjaxResult getBomDetail(
			@RequestParam(value="id") int id
			) {
		AjaxResult result = new AjaxResult();		
        result.data = this.bomService.getBomDetail(id);		
		return result;		
	}
	
	@RequestMapping("/bom_delete")
	public AjaxResult deleteBom(
			@RequestParam(value="id") int id
			) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();		
		AjaxResult result = new AjaxResult();		
		String sql = "delete from bom where id=:id ";
		paramMap.addValue("id", id);		
		int iRowEffected = this.sqlRunner.execute(sql, paramMap);
		String compsql = "delete from bom_comp where \"BOM_id\"=:id ";
		paramMap.addValue("id", id);
		int iRowEffected2 = this.sqlRunner.execute(compsql, paramMap);
		int totRowEffected;
		if(iRowEffected == 1 && iRowEffected2 == 1) {
			totRowEffected = 1;
		}else{
			totRowEffected = 0;
		}

		result.data = totRowEffected;
		return result;
	}
	
	@RequestMapping("/material_save")
	public AjaxResult bomComponentSave(
			@RequestParam(value="id" , required = false) Integer id,
			@RequestParam(value="BOM_id") int bom_id,
			@RequestParam(value="Material_id") int materialId,
			@RequestParam(value="Amount") float amt,
			@RequestParam(value="_order",required = false) Integer _order,
			@RequestParam(value="Description",required = false) String description,
			Authentication auth			
			) {
		
		User user = (User)auth.getPrincipal();
		AjaxResult result = new AjaxResult();
		
		BomComponent bomComponent = null;
		
		if (id !=null) {
			// 기존 데이터를 가져온다
			bomComponent = this.bomService.getBomComponent(id);			
		}else {
			//동일한 데이터가 있는지 검사해서 중복이 있으면 리턴
			//신규데이터를 등록한다
			boolean exists = this.bomService.checkDuplicateBomComponent(bom_id, materialId);
			if(exists) {
				result.success=false;
				result.message = "이미 존재하는 품목입니다.";
				return result;
			}			
			bomComponent = new BomComponent();
		}
		
		bomComponent.setBomId(bom_id);
		bomComponent.setMaterialId(materialId);
		bomComponent.setAmount(amt);
		bomComponent.set_order(_order);
		bomComponent.setDescription(description);		
		bomComponent.set_audit(user);
		
		bomComponent = this.bomService.saveBomComponent(bomComponent);
		result.data = bomComponent.getId();
		return result;		
	}
	
	@RequestMapping("/material_detail")
    public AjaxResult bomComponentDetail(
    		@RequestParam(value="id") int id    		
    		) {
    	AjaxResult result = new AjaxResult();    	
    	result.data = this.bomService.getBomComponentDetail(id);    	
    	return result;
    }	
	
	// BOM 삭제
	@PostMapping("/material_delete")
	public AjaxResult deleteBomComponent(
			@RequestParam(value="id") int id
			) {
		AjaxResult result = new AjaxResult();		
		result.data = this.bomService.deleteBomComponent(id);
		// bom_comp 테이블 삭제

		return result;		
	}
		
	
	@RequestMapping("/bom_comp_list")
	public AjaxResult getBomCompList(
			@RequestParam(value="id") Integer id
			) {
		AjaxResult result = new AjaxResult();
		String sql = """
	            select bc.id
	            , fn_code_name('mat_type', mg."MaterialType") as mat_type
	            , mg."Name" as group_name
	            , m."Name" as mat_name
	            , m."Code" as mat_code
	            , bc."Amount"
	            , bc."Material_id" as mat_id
	            , m."Unit_id"
	            , u."Name" as unit
	            , bc."Description"
	            , bc."_order" 
	            from bom_comp bc
	            left join material m on bc."Material_id"=m.id
	            left join unit u on u.id = m."Unit_id" 
	            left join mat_grp mg on m."MaterialGroup_id" =mg.id
	            where bc."BOM_id" = :bom_id
	            order by bc."_order"
	    """;		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("bom_id", id);
		result.data = this.sqlRunner.getRows(sql, paramMap);
		
		return result;		
	}
	
	@RequestMapping("/material_tree_list")
	public AjaxResult getComponentTreeList(
			@RequestParam(value="id") Integer id
			) {		
		AjaxResult result = new AjaxResult();		
		result.data = this.bomService.getBomComponentTreeList(id);		
		return result;		
	}	
	
	
	@PostMapping("/bom_replicate")
	public AjaxResult bomReplicate(
			@RequestParam(value="id") int bom_id,
			Authentication auth
			) {		
		
		User user = (User)auth.getPrincipal();				
		return this.bomService.bomReplicate(bom_id, user);
	}	
	
	@PostMapping("/bom_revision")
	public AjaxResult bomRevision(
			@RequestParam(value="id") int bom_id,
			Authentication auth
			) {		
		User user = (User)auth.getPrincipal();
		return this.bomService.bomRevision(bom_id, user);
	}

	@Transactional
	@PostMapping("/upload_save")
	public AjaxResult saveBomBulkData(
			@RequestParam("excelType") String excelType,
			@RequestParam("spjangcd") String spjangcd,
			@RequestParam("upload_file") MultipartFile upload_file,
			Authentication auth) throws IOException {

		User user = (User)auth.getPrincipal();
		Integer userId = user.getId();
		AjaxResult result = new AjaxResult();

		// 파일 저장
		DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
		String formattedDate = dtf.format(LocalDateTime.now());
		String upload_filename = settings.getProperty("file_temp_upload_path") + formattedDate + "_" + upload_file.getOriginalFilename();
		File file = new File(upload_filename);
		if (file.exists()) file.delete();
		try (FileOutputStream destination = new FileOutputStream(upload_filename)) {
			destination.write(upload_file.getBytes());
		}

		// 공통 날짜
		LocalDateTime now = LocalDateTime.now();
		Timestamp startDate = Timestamp.valueOf(LocalDate.now().atStartOfDay());
		Timestamp endDate = Timestamp.valueOf("2100-12-31 00:00:00");

		// Unit명 → id 맵
		Map<String, Integer> unitNameToIdMap = new HashMap<>();
		unitNameToIdMap.put("%", 13);
		unitNameToIdMap.put("Batch", 11);
		unitNameToIdMap.put("BOX", 1);
		unitNameToIdMap.put("℃", 9);
		unitNameToIdMap.put("cm", 2);
		unitNameToIdMap.put("cp", 12);
		unitNameToIdMap.put("EA", 3);
		unitNameToIdMap.put("g", 4);
		unitNameToIdMap.put("kg", 5);
		unitNameToIdMap.put("KOHmg/g", 10);
		unitNameToIdMap.put("ML", 18);
		unitNameToIdMap.put("pack", 14);
		unitNameToIdMap.put("ROLL", 7);
		unitNameToIdMap.put("RPM", 8);
		unitNameToIdMap.put("seconds", 15);
		unitNameToIdMap.put("t", 6);

		if ("00".equals(excelType)) {
			// ===== 제일전기 =====
			List<List<String>> all_rows = this.bomUploadService.excel_read(upload_filename);
			// 제품
			List<String> productNames = new ArrayList<>();
			List<String> productRow = all_rows.get(0); // 1번째 행
			int productStartCol = 12; // M열 (index 12)
			for (int col = productStartCol; col < productRow.size(); col++) {
				String name = productRow.get(col);
				if (name == null || name.trim().isEmpty()) break;
				productNames.add(name.replaceAll("[\\r\\n]+", " ").trim());
			}
			// 자재
			List<String> materialNames = new ArrayList<>();
			int materialStartRow = 1; // 2번째 행부터
			int materialNameCol = 9;   // J열 (index 9)
			String lastMaterialName = null;
			for (int rowIdx = materialStartRow; rowIdx < all_rows.size(); rowIdx++) {
				List<String> row = all_rows.get(rowIdx);
				if (row.size() <= materialNameCol) break;
				String matName = row.get(materialNameCol);
				if (matName != null) matName = matName.replaceAll("[\\r\\n]+", " ").trim();
				if (matName == null || matName.trim().isEmpty()) matName = lastMaterialName;
				else lastMaterialName = matName.trim();
				materialNames.add(matName.trim());
			}
			// 제품 material 등록/조회
			Map<String, Integer> productNameToId = new HashMap<>();
			for (String pname : productNames) {
				productNameToId.put(pname, getOrCreateProductId(pname, spjangcd));
			}
			// 자재 material 등록/조회
			Map<String, Integer> materialNameToId = new HashMap<>();
			for (String mname : materialNames) {
				if (!materialNameToId.containsKey(mname)) {
					// 최초 등장한 자재명만 id 생성(중복 insert 방지)
					Integer mid = getOrCreateMaterialId(mname, spjangcd);
					materialNameToId.put(mname, mid);
				}
			}

			List<Bom> bomList = new ArrayList<>();
			for (String productName : productNames) {
				Integer productId = productNameToId.get(productName);
				Bom bom = bomRepository.findByMaterialIdAndBomTypeAndVersion(productId, "manufacturing", "1.0");
				if (bom == null) {
					bom = new Bom();
					bom.setName(productName);
					bom.setMaterialId(productId);
					bom.setBomType("manufacturing");
					bom.setVersion("1.0");
					bom.setStartDate(startDate);
					bom.setOutputAmount(1F);
					bom.setEndDate(endDate);
					bom.setSpjangcd(spjangcd);
					bom.set_creater_id(userId);
					bom.set_created(startDate);
					bom = bomRepository.save(bom);
				}
				bomList.add(bom); // **기존이든 신규든 무조건 추가! (이게 핵심)**
			}

			// BOM Component 중복(동일 materialId) 누적/합산
			for (int pIdx = 0; pIdx < productNames.size(); pIdx++) {
				Bom bom = bomList.get(pIdx);

				// 중복 자재 합산 map
				Map<Integer, BomComponent> bomCompMap = new HashMap<>();

				for (int mIdx = 0; mIdx < materialNames.size(); mIdx++) {
					List<String> row = all_rows.get(1 + mIdx);
					int cellIdx = productStartCol + pIdx;
					if (row.size() <= cellIdx) continue;
					String qtyStr = row.get(cellIdx);
					float qty = 0f;
					try {
						qty = Float.parseFloat((qtyStr == null || qtyStr.trim().isEmpty()) ? "0" : qtyStr.trim());
					} catch (Exception ignore) {
						qty = 0f;
					}

					if (qty <= 0f) continue; // 그래도 행 단위로 0인 건 건너뛰는 건 유지 가능

					String description = "";
					if (row.size() > 10 && row.get(10) != null)
						description = row.get(10).replaceAll("[\\r\\n]+", " ").trim();

					String materialName = materialNames.get(mIdx);
					Integer materialId = materialNameToId.get(materialName);

					// 중복 자재 합산
					BomComponent comp = bomCompMap.get(materialId);
					if (comp == null) {
						comp = new BomComponent();
						comp.setBomId(bom.getId());
						comp.setMaterialId(materialId);
						comp.setAmount((float) qty);
						comp.set_creater_id(userId);
						comp.set_created(startDate);
						comp.set_order(1);
						comp.setSpjangcd(spjangcd);
						comp.setDescription(description);
						bomCompMap.put(materialId, comp);
					} else {
						comp.setAmount(comp.getAmount() + (float) qty);
						if (qty > 0 && description != null && !description.trim().isEmpty()) {
							if (comp.getDescription() == null || comp.getDescription().trim().isEmpty())
								comp.setDescription(description);
							else
								comp.setDescription(comp.getDescription() + ", " + description);
						}
					}

				}
				// 기존: for (BomComponent comp : bomCompMap.values()) { ... 저장 ... }
				List<BomComponent> validComponents = bomCompMap.values().stream()
						.filter(c -> Optional.ofNullable(c.getAmount()).orElse(0f) > 0f)
						.collect(Collectors.toList());

				bomComponentRepository.deleteByBomId(bom.getId());
				bomComponentRepository.flush();
				bomComponentRepository.saveAll(validComponents);

			}
			result.success = true;
			result.data = bomList;

		}  else if ("01".equals(excelType)) {
			// ===== 대양전기 =====
			List<List<String>> all_rows = this.bomUploadService.excel_read(upload_filename);

			// 제품명: 0번째 행, 1번째 열 (B1)
			String productName = all_rows.get(0).get(1).replaceAll("[\\r\\n]+", " ").trim();
			int productId = getOrCreateProductId_Daeyang(productName, spjangcd);

			Bom bom = bomRepository.findByMaterialIdAndBomTypeAndVersion(productId, "manufacturing", "1.0");
			if (bom == null) {
				bom = new Bom();
				bom.setName(productName);
				bom.setMaterialId(productId);
				bom.setBomType("manufacturing");
				bom.setVersion("1.0");
				bom.setStartDate(startDate);
				bom.setOutputAmount(1F);
				bom.setEndDate(endDate);
				bom.setSpjangcd(spjangcd);
				bom.set_creater_id(userId);
				bom.set_created(startDate);
				bom = bomRepository.save(bom);
			}

			Map<Integer, BomComponent> bomCompMap = new HashMap<>();
			Map<String, Integer> materialNameToId = new HashMap<>(); // 자재명 → id 캐시
			// 3번째 행(index=2)부터 끝까지 반복 (자재 정보)
			for (int r = 2; r < all_rows.size(); r++) {
				List<String> row = all_rows.get(r);
				if (row.size() < 4 || row.get(1) == null || row.get(1).trim().isEmpty()) continue;

				// 그룹정보(구분) 수집
				String groupStr = row.get(0) != null ? row.get(0).trim() : "";
				int materialGroupId = 48; // 구분이 SMD, 대양 둘다 아닐경우 48
				if (groupStr.equals("SMD 동영자재")) materialGroupId = 44;
				else if (groupStr.equals("대양 수삽자재")) materialGroupId = 49;

				// 자재명 수집
				String materialName = row.get(1) != null ? row.get(1).replaceAll("[\\r\\n]+", " ").trim() : "";
				// 자재단위 수집
				String unitName = row.get(2) != null ? row.get(2).trim() : "";
				int unitId = unitNameToIdMap.getOrDefault(unitName, 3);

				// 자재 id 생성/조회
				Integer materialId;
				String key = materialName + "|" + materialGroupId + "|" + unitId;
				if (materialNameToId.containsKey(key)) {
					materialId = materialNameToId.get(key);
				} else {
					materialId = getOrCreateMaterialId_Daeyang(materialName, spjangcd, materialGroupId, unitId);
					materialNameToId.put(key, materialId);
				}
				// 필요수량 수집
				// 수량/설명
				String qtyStr = row.get(3) != null ? row.get(3).trim() : "";
				float amount = 0f;
				try { amount = Float.parseFloat(qtyStr); }
				catch(Exception e) { amount = 0f; }

				if (amount <= 0) continue; // 수량이 0인 데이터 무시

				// 위치정보(비고) 수집
				String location = row.size() > 5 && row.get(5) != null ? row.get(5).replaceAll("[\\r\\n]+", " ").trim() : "";

				BomComponent comp = bomCompMap.get(materialId);
				if (comp == null) {
					comp = new BomComponent();
					comp.setBomId(bom.getId());
					comp.setMaterialId(materialId);
					comp.setAmount(amount);
					comp.set_creater_id(userId);
					comp.set_created(startDate);
					comp.set_order(1);
					comp.setSpjangcd(spjangcd);
					comp.setDescription(location);
					bomCompMap.put(materialId, comp);
				} else {
					comp.setAmount(comp.getAmount() + amount);
					if (amount > 0 && location != null && !location.trim().isEmpty()) {
						if (comp.getDescription() == null || comp.getDescription().trim().isEmpty())
							comp.setDescription(location);
						else
							comp.setDescription(comp.getDescription() + ", " + location);
					}
				}
			}
			bomComponentRepository.deleteByBomId(bom.getId());
			bomComponentRepository.flush();
			// 수량이 0 초과인 것만 저장
			bomComponentRepository.saveAll(
					bomCompMap.values().stream()
							.filter(c -> Optional.ofNullable(c.getAmount()).orElse(0f) > 0)
							.collect(Collectors.toList())
			);
			result.success = true;
			result.data = bom;
		}  else if ("02".equals(excelType)) {
			// ===== 기타 =====
			List<List<String>> all_rows = this.bomUploadService.excel_read(upload_filename);

			// 제품명: 0번째 행, 1번째 열 (B1)
			String productName = all_rows.get(0).get(1).replaceAll("[\\r\\n]+", " ").trim();
			int productId = getOrCreateProductId_Acro(productName, spjangcd);

			Bom bom = bomRepository.findByMaterialIdAndBomTypeAndVersion(productId, "manufacturing", "1.0");
			if (bom == null) {
				bom = new Bom();
				bom.setName(productName);
				bom.setMaterialId(productId);
				bom.setBomType("manufacturing");
				bom.setVersion("1.0");
				bom.setStartDate(startDate);
				bom.setOutputAmount(1F);
				bom.setEndDate(endDate);
				bom.setSpjangcd(spjangcd);
				bom.set_creater_id(userId);
				bom.set_created(startDate);
				bom = bomRepository.save(bom);
			}

			Map<Integer, BomComponent> bomCompMap = new HashMap<>();
			Map<String, Integer> materialNameToId = new HashMap<>(); // 자재명 → id 캐시
			// 3번째 행(index=2)부터 끝까지 반복 (자재 정보)
			for (int r = 2; r < all_rows.size(); r++) {
				List<String> row = all_rows.get(r);
				if (row.size() < 4 || row.get(1) == null || row.get(1).trim().isEmpty()) continue;

				// 그룹정보(구분) 수집
				String groupStr = row.get(0) != null ? row.get(0).trim() : "";
				int materialGroupId = 55; // 구분 아크로SMD자재 고정

				// 자재명 수집
				String materialName = row.get(1) != null ? row.get(1).replaceAll("[\\r\\n]+", " ").trim() : "";
				// 자재단위 수집
				String unitName = row.get(2) != null ? row.get(2).trim() : "";
				int unitId = unitNameToIdMap.getOrDefault(unitName, 3);

				// 자재 id 생성/조회
				Integer materialId;
				String key = materialName + "|" + materialGroupId + "|" + unitId;
				if (materialNameToId.containsKey(key)) {
					materialId = materialNameToId.get(key);
				} else {
					materialId = getOrCreateMaterialId_Acro(materialName, spjangcd, materialGroupId, unitId);
					materialNameToId.put(key, materialId);
				}
				// 필요수량 수집
				// 수량/설명
				String qtyStr = row.get(3) != null ? row.get(3).trim() : "";
				float amount = 0f;
				try { amount = Float.parseFloat(qtyStr); }
				catch(Exception e) { amount = 0f; }

				if (amount <= 0) continue; // 수량이 0인 데이터 무시

				// 위치정보(비고) 수집
				String location = row.size() > 5 && row.get(5) != null ? row.get(5).replaceAll("[\\r\\n]+", " ").trim() : "";

				BomComponent comp = bomCompMap.get(materialId);
				if (comp == null) {
					comp = new BomComponent();
					comp.setBomId(bom.getId());
					comp.setMaterialId(materialId);
					comp.setAmount(amount);
					comp.set_creater_id(userId);
					comp.set_created(startDate);
					comp.set_order(1);
					comp.setSpjangcd(spjangcd);
					comp.setDescription(location);
					bomCompMap.put(materialId, comp);
				} else {
					comp.setAmount(comp.getAmount() + amount);
					if (amount > 0 && location != null && !location.trim().isEmpty()) {
						if (comp.getDescription() == null || comp.getDescription().trim().isEmpty())
							comp.setDescription(location);
						else
							comp.setDescription(comp.getDescription() + ", " + location);
					}
				}
			}
			bomComponentRepository.deleteByBomId(bom.getId());
			bomComponentRepository.flush();
			// 수량이 0 초과인 것만 저장
			bomComponentRepository.saveAll(
					bomCompMap.values().stream()
							.filter(c -> Optional.ofNullable(c.getAmount()).orElse(0f) > 0)
							.collect(Collectors.toList())
			);
			result.success = true;
			result.data = bom;
		}

		// 파일 삭제
		File uploadedFile = new File(upload_filename);
		if (uploadedFile.exists()) uploadedFile.delete();

		return result;
	}
	// 대양전기 제품 등록
	@Transactional
	public Integer getOrCreateProductId_Daeyang(String productName, String spjangcd) {
		String cleanName = productName.trim();
		Material prod = materialRepository.findByNameTrimmed(cleanName);

		if (prod != null) return prod.getId();

		// 대양전기 전용 materialGroupId 예시 (47로 지정, 필요시 수정)
		Material newProd = new Material();
		newProd.setName(productName);
		newProd.setMaterialGroupId(45); // <-- 대양전기 그룹ID로
		newProd.setCode(getNextMaterialCode());
		newProd.set_created(Timestamp.valueOf(LocalDateTime.now()));
		newProd.setFactory_id(1);
		newProd.setUnitId(3); // 기본단위
		newProd.setLotUseYn("0");
		newProd.setMtyn("1");
		newProd.setUseyn("0");
		newProd.setPurchaseOrderStandard("mrp");
		newProd.setSpjangcd(spjangcd);
		newProd.setWorkCenterId(39);
		newProd.setStoreHouseId(4);
		newProd = materialRepository.save(newProd);
		return newProd.getId();
	}
	// --- 대양전기: 자재 신규 등록/조회 (GroupId/UnitId 파라미터) ---
	@Transactional
	public Integer getOrCreateMaterialId_Daeyang(String materialName, String spjangcd, int materialGroupId, int unitId) {
		String cleanName = materialName.trim();
		Material mat = materialRepository.findByNameTrimmed(cleanName);
		if (mat != null) return mat.getId();
		Material newMat = new Material();
		newMat.setName(materialName);
		newMat.setMaterialGroupId(materialGroupId);
		newMat.setUnitId(unitId);
		newMat.set_created(Timestamp.valueOf(LocalDateTime.now()));
		newMat.setSpjangcd(spjangcd);
		// ... 추가 필드
		newMat.setCode(getNextMaterialCode()); // '4000' + N
		newMat.setLotUseYn("0");
		newMat.setMtyn("1");
		newMat.setUseyn("0");
		newMat.setPurchaseOrderStandard("mrp");
		newMat.setWorkCenterId(39);
		newMat.setStoreHouseId(3);
		newMat.setFactory_id(1);
		newMat = materialRepository.save(newMat);
		return newMat.getId();
	}
	// 기타(아크로) 제품 등록
	@Transactional
	public Integer getOrCreateProductId_Acro(String productName, String spjangcd) {
		String cleanName = productName.trim();
		Material prod = materialRepository.findByNameTrimmed(cleanName);

		if (prod != null) return prod.getId();

		// 기타(아크로) 전용 materialGroupId 예시 (47로 지정, 필요시 수정)
		Material newProd = new Material();
		newProd.setName(productName);
		newProd.setMaterialGroupId(52); // <-- 대양전기 그룹ID로
		newProd.setCode(getNextMaterialCode());
		newProd.set_created(Timestamp.valueOf(LocalDateTime.now()));
		newProd.setFactory_id(1);
		newProd.setUnitId(3); // 기본단위
		newProd.setLotUseYn("0");
		newProd.setMtyn("1");
		newProd.setUseyn("0");
		newProd.setPurchaseOrderStandard("mrp");
		newProd.setSpjangcd(spjangcd);
		newProd.setWorkCenterId(39);
		newProd.setStoreHouseId(4);
		newProd = materialRepository.save(newProd);
		return newProd.getId();
	}
	// --- 기타(아크로): 자재 신규 등록/조회 (GroupId/UnitId 파라미터) ---
	@Transactional
	public Integer getOrCreateMaterialId_Acro(String materialName, String spjangcd, int materialGroupId, int unitId) {
		String cleanName = materialName.trim();
		Material mat = materialRepository.findByNameTrimmed(cleanName);
		if (mat != null) return mat.getId();
		Material newMat = new Material();
		newMat.setName(materialName);
		newMat.setMaterialGroupId(materialGroupId);
		newMat.setUnitId(unitId);
		newMat.set_created(Timestamp.valueOf(LocalDateTime.now()));
		newMat.setSpjangcd(spjangcd);
		// ... 추가 필드
		newMat.setCode(getNextMaterialCode()); // '4000' + N
		newMat.setLotUseYn("0");
		newMat.setMtyn("1");
		newMat.setUseyn("0");
		newMat.setPurchaseOrderStandard("mrp");
		newMat.setWorkCenterId(39);
		newMat.setStoreHouseId(3);
		newMat.setFactory_id(1);
		newMat = materialRepository.save(newMat);
		return newMat.getId();
	}
	// 제일전기 제품 조회/등록
	@Transactional
	public Integer getOrCreateProductId(String productName, String spjangcd) {
		String cleanName = productName.trim();
		Material prod = materialRepository.findByNameTrimmed(cleanName);

		if (prod != null) return prod.getId();

		// 신규 등록: 제품 (materialGroupId=46, Code 자동)
		Material newProd = new Material();
		newProd.setName(productName);
		newProd.setMaterialGroupId(46);
		newProd.setCode(getNextMaterialCode()); // '4000' + N
		newProd.set_created(Timestamp.valueOf(LocalDateTime.now()));
		newProd.setFactory_id(1);
		newProd.setUnitId(3);
		newProd.setLotUseYn("0");
		newProd.setMtyn("1");
		newProd.setUseyn("0");
		newProd.setPurchaseOrderStandard("mrp");
		newProd.setSpjangcd(spjangcd);
		newProd.setWorkCenterId(39);
		newProd.setStoreHouseId(4);
		newProd = materialRepository.save(newProd);
		return newProd.getId();
	}
	// 제일전기 자재 조회/등록
	@Transactional
	public Integer getOrCreateMaterialId(String materialName, String spjangcd) {
		String cleanName = materialName.trim();
		Material mat = materialRepository.findByNameTrimmed(cleanName);
		if (mat != null) return mat.getId();

		// 신규 등록: 자재 (materialGroupId=50, Code 자동)
		Material newMat = new Material();
		newMat.setName(materialName);
		newMat.setMaterialGroupId(50);
		newMat.setCode(getNextMaterialCode()); // '4000' + N
		newMat.set_created(Timestamp.valueOf(LocalDateTime.now()));
		newMat.setFactory_id(1);
		newMat.setUnitId(3);
		newMat.setSpjangcd(spjangcd);
		newMat.setLotUseYn("0");
		newMat.setMtyn("1");
		newMat.setUseyn("0");
		newMat.setPurchaseOrderStandard("mrp");
		newMat.setWorkCenterId(39);
		newMat.setStoreHouseId(3);
		newMat = materialRepository.save(newMat);
		return newMat.getId();
	}

	/** material.code의 다음 '4000'+N 값을 생성하는 메서드 (실제 구현 필요!) */
	public String getNextMaterialCode() {
		String maxCode = materialRepository.findMaxCodeBy4000Prefix();
		int nextNumber = 4000;
		if (maxCode != null && !maxCode.isEmpty()) {
			try {
				int codeNum = Integer.parseInt(maxCode);
				nextNumber = codeNum + 1;
			} catch (NumberFormatException ignore) {}
		}

		// 최종 insert 직전 중복 체크
		while (materialRepository.existsByCode(String.valueOf(nextNumber))) {
			nextNumber++;
		}
		return String.valueOf(nextNumber);
	}



}