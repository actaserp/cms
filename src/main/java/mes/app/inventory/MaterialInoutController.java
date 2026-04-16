package mes.app.inventory;

import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import javax.servlet.http.HttpServletRequest;
import javax.transaction.Transactional;

import lombok.extern.slf4j.Slf4j;
import mes.domain.entity.*;
import mes.domain.repository.*;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.core.Authentication;
import org.springframework.util.MultiValueMap;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import mes.app.inventory.service.LotService;
import mes.app.inventory.service.MaterialInoutService;
import mes.domain.model.AjaxResult;
import mes.domain.services.CommonUtil;

@Slf4j
@RestController
@RequestMapping("/api/inventory/material_inout")
public class MaterialInoutController {
	
	@Autowired
	private MaterialInoutService materialInoutService;
	
	@Autowired
	private LotService lotService;
	
	@Autowired
	MatInoutRepository matInoutRepository;
	
	@Autowired
	MaterialRepository materialRepository;
	
	@Autowired
	MatLotRepository matLotRepository;
	
	@Autowired
	TestResultRepository testResultRepository;
	
	@Autowired
	TestItemResultRepository testItemResultRepository;

	@Autowired
	BujuRepository bujuRepository;
    @Autowired
    private JdbcTemplate jdbcTemplate;

	// 입출고 전체 리스트
	@GetMapping("/read")
	public AjaxResult getMaterialInout(
			@RequestParam(value = "srchStartDt", required=false) String srchStartDt,
			@RequestParam(value = "srchEndDt", required=false) String srchEndDt,
			@RequestParam(value = "house_pk", required=false) String housePk,
			@RequestParam(value = "mat_type", required=false) String matType,
			@RequestParam(value = "mat_grp_pk", required=false) String matGrpPk,
			@RequestParam(value = "spjangcd", required=false) String spjangcd,
			@RequestParam(value = "keyword", required=false) String keyword) {
		
        List<Map<String, Object>> items = this.materialInoutService.getMaterialInout(srchStartDt,srchEndDt,housePk,matType,matGrpPk,keyword,spjangcd);
   		
        AjaxResult result = new AjaxResult();
        result.data = items;        				
        
		return result;
	}

	// 입출고 전체 리스트
	@GetMapping("/read_receipt")
	public AjaxResult getMaterialInout_receipt(
			@RequestParam(value = "srchStartDt", required=false) String srchStartDt,
			@RequestParam(value = "srchEndDt", required=false) String srchEndDt,
			@RequestParam(value = "house_pk", required=false) String housePk,
			@RequestParam(value = "mat_type", required=false) String matType,
			@RequestParam(value = "mat_grp_pk", required=false) String matGrpPk,
			@RequestParam(value = "spjangcd", required=false) String spjangcd,
			@RequestParam(value = "keyword", required=false) String keyword) {

		List<Map<String, Object>> items = this.materialInoutService.getMaterialInoutReceipt(srchStartDt,srchEndDt,housePk,matType,matGrpPk,keyword,spjangcd);

		AjaxResult result = new AjaxResult();
		result.data = items;

		return result;
	}

	// 불출 리스트
	@GetMapping("/read_issue")
	public AjaxResult getMaterialInout_issue(
			@RequestParam(value = "srchStartDt", required=false) String srchStartDt,
			@RequestParam(value = "srchEndDt", required=false) String srchEndDt,
			@RequestParam(value = "house_pk", required=false) String housePk,
			@RequestParam(value = "mat_type", required=false) String matType,
			@RequestParam(value = "mat_grp_pk", required=false) String matGrpPk,
			@RequestParam(value = "spjangcd", required=false) String spjangcd,
			@RequestParam(value = "keyword", required=false) String keyword) {

		List<Map<String, Object>> items = this.materialInoutService.getMaterialInoutIssue(srchStartDt,srchEndDt,housePk,matType,matGrpPk,keyword,spjangcd);

		AjaxResult result = new AjaxResult();
		result.data = items;

		return result;
	}

	// 폐기 리스트
	@GetMapping("/read_disposal")
	public AjaxResult getMaterialInout_disposal(
			@RequestParam(value = "srchStartDt", required=false) String srchStartDt,
			@RequestParam(value = "srchEndDt", required=false) String srchEndDt,
			@RequestParam(value = "house_pk", required=false) String housePk,
			@RequestParam(value = "mat_type", required=false) String matType,
			@RequestParam(value = "mat_grp_pk", required=false) String matGrpPk,
			@RequestParam(value = "spjangcd", required=false) String spjangcd,
			@RequestParam(value = "keyword", required=false) String keyword) {

		List<Map<String, Object>> items = this.materialInoutService.getMaterialInoutDisposal(srchStartDt,srchEndDt,housePk,matType,matGrpPk,keyword,spjangcd);

		AjaxResult result = new AjaxResult();
		result.data = items;

		return result;
	}
	
//	@PostMapping("/save")
//	@Transactional
//	public AjaxResult saveMaterialInout(
//			@RequestParam("Description") String description,
//			@RequestParam("InoutQty") String inoutQty,
//			@RequestParam("InoutType") String inoutType,
//			@RequestParam("Material_id") String materialId,
//			@RequestParam("StoreHouse_id") String storeHouseId,
//			@RequestParam("inoutDate") String inoutDateStr,
//			@RequestParam(value = "mio_pk", required = false) Integer mio_pk,
//			@RequestParam("cboMaterialGroup") String cboMaterialGroup,
//			@RequestParam("cboMaterialType") String cboMaterialType,
//			@RequestParam("type") String type,
//			@RequestParam("spjangcd") String spjangcd,
//			HttpServletRequest request,
//			Authentication auth) {
//
//		User user = (User)auth.getPrincipal();
//
//		AjaxResult result = new AjaxResult();
//
//		Integer matPk = Integer.parseInt(materialId);
//		String state = "confirmed";
//		String _status = "a";
//		int qty = Integer.parseInt(
//				inoutQty.replace(",", "").replaceAll("[^\\d-]", "")
//		);
//
//		result.success = false;
//
//		boolean isUpdate = false;
//
//		MaterialInout mi;
//		if (mio_pk != null) {
//			isUpdate = true;
//			mi = matInoutRepository.findById(mio_pk)
//					.orElseThrow(() -> new RuntimeException("기존 데이터 없음: " + mio_pk));
//		} else {
//            mi = new MaterialInout();
//		}
//
//		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm");
//		LocalDateTime dateTime = LocalDateTime.parse(inoutDateStr, formatter);
//
//		mi.setInoutDate(dateTime.toLocalDate());
//		mi.setInoutTime(dateTime.toLocalTime());
//		mi.setMaterialId(matPk);
//		mi.setStoreHouseId(Integer.parseInt(storeHouseId));
//
//		Material m = this.materialRepository.getMaterialById(matPk);
//
//		String testYn = m.getInTestYN() != null ? m.getInTestYN() : "";
//
//		if (type.equals("in")) {
//			mi.setInOut("in");
//			mi.setInputType(inoutType);
//			if(testYn.equals("Y") && !isUpdate) {
//				mi.setPotentialInputQty((float)qty);
//				state = "waiting";
//				_status = "t";
//			} else {
//				mi.setInputQty((float)qty);
//				mi.setOutputQty(0f);
//				mi.setOutputType("");
//			}
//		} else if(type.equals("recall")){
//			mi.setInOut("recall");
//			mi.setOutputType(inoutType);
//			mi.setOutputQty((float)qty);
//			mi.setInputQty(0f);
//			mi.setInputType("");
//
//		} else if(type.equals("return")){
//			mi.setInOut("return");
//			mi.setInputType(inoutType);
//			mi.setInputQty((float)qty);
//			mi.setOutputQty(0f);
//			mi.setOutputType("");
//
//		} else  {
//			mi.setInOut("out");
//			mi.setOutputType(inoutType);
//			mi.setOutputQty((float)qty);
//			mi.setInputQty(0f);
//			mi.setInputType("");
//		}
//		mi.setDescription(description);
//		mi.setState(state);
//		mi.set_status(_status);
//		mi.set_audit(user);
//		mi.setSpjangcd(spjangcd);
//
//		this.matInoutRepository.save(mi);
//		this.matInoutRepository.flush();
//
//
////		jdbcTemplate.query(
////				"SELECT sp_update_mat_in_house_by_inout(?, ?)",
////				rs -> {},  // 결과 무시
////				matPk, Integer.parseInt(storeHouseId)
////		);
//
//		result.success = true;
//
//		return result;
//	}

	@PostMapping("/save")
	@Transactional
	public AjaxResult saveMaterialInout(
			@RequestParam("Description") String description,
			@RequestParam("InoutQty") String inoutQty,
			@RequestParam("InoutType_hidden") String inoutType,
			@RequestParam(value="cboCompany", required = false) Integer companyId,
			@RequestParam("Material_id") String materialId,
			@RequestParam("StoreHouse_id") String storeHouseId,
			@RequestParam("inoutDate") String inoutDateStr,
			@RequestParam(value = "mio_pk", required = false) Integer mio_pk,
			@RequestParam("cboMaterialGroup") String cboMaterialGroup,
			@RequestParam("cboMaterialType") String cboMaterialType,
			@RequestParam("type") String type,
			@RequestParam("spjangcd") String spjangcd,
			HttpServletRequest request,
			Authentication auth) {

		User user = (User)auth.getPrincipal();

		AjaxResult result = new AjaxResult();

		Integer matPk = Integer.parseInt(materialId);
		String state = "confirmed";
		String _status = "a";
		int qty = Integer.parseInt(
				inoutQty.replace(",", "").replaceAll("[^\\d-]", "")
		);

		result.success = false;

		boolean isUpdate = false;

		MaterialInout mi;
		if (mio_pk != null) {
			isUpdate = true;
			mi = matInoutRepository.findById(mio_pk)
					.orElseThrow(() -> new RuntimeException("기존 데이터 없음: " + mio_pk));
		} else {
			mi = new MaterialInout();
		}

		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm");
		LocalDateTime dateTime = LocalDateTime.parse(inoutDateStr, formatter);

		mi.setInoutDate(dateTime.toLocalDate());
		mi.setInoutTime(dateTime.toLocalTime());
		mi.setCompanyId(companyId);
		mi.setMaterialId(matPk);
		mi.setStoreHouseId(Integer.parseInt(storeHouseId));

		Material m = this.materialRepository.getMaterialById(matPk);

//		String testYn = m.getInTestYN() != null ? m.getInTestYN() : "";
		String testYn = Optional.ofNullable(m.getInTestYN()).orElse("").trim();
		String curState = Optional.ofNullable(mi.getState()).orElse("").trim();
		boolean isTest = "Y".equalsIgnoreCase(testYn);
		boolean isWaiting = "waiting".equalsIgnoreCase(curState);
		log.info("isTest={}, curState='{}'(len={}), isWaiting={}", isTest, curState, curState.length(), isWaiting);

        switch (type) {
            case "in" -> {
                mi.setInOut("in");
                mi.setInputType(inoutType);

//                boolean isWaiting = mi.getState() != null && mi.getState().equals("waiting");

                if (testYn.equals("Y")) {
                    mi.setPotentialInputQty((float) qty);
                    state = "waiting";
                    _status = "t";
                } else {
                    mi.setInputQty((float) qty);
                    mi.setOutputQty(0f);
                    mi.setOutputType("");
                }
            }
            case "recall" -> {
                mi.setInOut("recall");
                mi.setOutputType(inoutType);
                mi.setOutputQty((float) qty);
                mi.setInputQty(0f);
                mi.setInputType("");
            }
            case "return" -> {
                mi.setInOut("return");
                mi.setInputType(inoutType);
                mi.setInputQty((float) qty);
                mi.setOutputQty(0f);
                mi.setOutputType("");
            }
            default -> {
                mi.setInOut("out");
                mi.setOutputType(inoutType);
                mi.setOutputQty((float) qty);
                mi.setInputQty(0f);
                mi.setInputType("");
            }
        }
		mi.setDescription(description);
		mi.setState(state);
		mi.set_status(_status);
		mi.set_audit(user);
		mi.setSpjangcd(spjangcd);

		this.matInoutRepository.save(mi);
		this.matInoutRepository.flush();


//		jdbcTemplate.query(
//				"SELECT sp_update_mat_in_house_by_inout(?, ?)",
//				rs -> {},  // 결과 무시
//				matPk, Integer.parseInt(storeHouseId)
//		);

		result.success = true;

		return result;
	}

	@PostMapping("/save_nocomp")
	@Transactional
	public AjaxResult saveMaterialInout_noComp(
			@RequestParam("Description") String description,
			@RequestParam("InoutQty") String inoutQty,
			@RequestParam("InoutType_hidden") String inoutType,
			@RequestParam("Material_id") String materialId,
			@RequestParam("StoreHouse_id") String storeHouseId,
			@RequestParam("inoutDate") String inoutDateStr,
			@RequestParam(value = "mio_pk", required = false) Integer mio_pk,
			@RequestParam("cboMaterialGroup") String cboMaterialGroup,
			@RequestParam("cboMaterialType") String cboMaterialType,
			@RequestParam("type") String type,
			@RequestParam("spjangcd") String spjangcd,
			HttpServletRequest request,
			Authentication auth) {

		User user = (User)auth.getPrincipal();

		AjaxResult result = new AjaxResult();

		Integer matPk = Integer.parseInt(materialId);
		String state = "confirmed";
		String _status = "a";
		int qty = Integer.parseInt(
				inoutQty.replace(",", "").replaceAll("[^\\d-]", "")
		);

		result.success = false;

		boolean isUpdate = false;

		MaterialInout mi;
		if (mio_pk != null) {
			isUpdate = true;
			mi = matInoutRepository.findById(mio_pk)
					.orElseThrow(() -> new RuntimeException("기존 데이터 없음: " + mio_pk));
		} else {
			mi = new MaterialInout();
		}

		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm");
		LocalDateTime dateTime = LocalDateTime.parse(inoutDateStr, formatter);

		mi.setInoutDate(dateTime.toLocalDate());
		mi.setInoutTime(dateTime.toLocalTime());
		mi.setMaterialId(matPk);
		mi.setStoreHouseId(Integer.parseInt(storeHouseId));

		Material m = this.materialRepository.getMaterialById(matPk);

		String testYn = m.getInTestYN() != null ? m.getInTestYN() : "";

		if (type.equals("in")) {
			mi.setInOut("in");
			mi.setInputType(inoutType);

			boolean isWaiting = mi.getState() != null && mi.getState().equals("waiting");

			if(testYn.equals("Y") && isWaiting) {
				mi.setPotentialInputQty((float)qty);
				state = "waiting";
				_status = "t";
			} else {
				mi.setInputQty((float)qty);
				mi.setOutputQty(0f);
				mi.setOutputType("");
			}
		} else if(type.equals("recall")){
			mi.setInOut("recall");
			mi.setOutputType(inoutType);
			mi.setOutputQty((float)qty);
			mi.setInputQty(0f);
			mi.setInputType("");

		} else if(type.equals("return")){
			mi.setInOut("return");
			mi.setInputType(inoutType);
			mi.setInputQty((float)qty);
			mi.setOutputQty(0f);
			mi.setOutputType("");

		} else  {
			mi.setInOut("out");
			mi.setOutputType(inoutType);
			mi.setOutputQty((float)qty);
			mi.setInputQty(0f);
			mi.setInputType("");
		}
		mi.setDescription(description);
		mi.setState(state);
		mi.set_status(_status);
		mi.set_audit(user);
		mi.setSpjangcd(spjangcd);

		this.matInoutRepository.save(mi);
		this.matInoutRepository.flush();


//		jdbcTemplate.query(
//				"SELECT sp_update_mat_in_house_by_inout(?, ?)",
//				rs -> {},  // 결과 무시
//				matPk, Integer.parseInt(storeHouseId)
//		);

		result.success = true;

		return result;
	}

	@GetMapping("/matinout_detail")
	public AjaxResult getMaterialInoutDetail(
			@RequestParam(value = "mio_pk", required=false) Integer mio_pk) {

		List<Map<String, Object>> items = materialInoutService.getMaterialInoutDetail(mio_pk);

		AjaxResult result = new AjaxResult();
		result.data = items;

		return result;
	}

	@PostMapping("/delete")
	public AjaxResult getInoutDelete(@RequestBody Map<String, Object> body) {
		Integer mio_pk = Integer.valueOf(body.get("mio_pk").toString());

		AjaxResult result = new AjaxResult();

		MaterialInout mi = matInoutRepository.findById(mio_pk)
					.orElseThrow(() -> new RuntimeException("기존 데이터 없음: " + mio_pk));

		Integer matPk = mi.getMaterialId();
		Integer storeHouseId = mi.getStoreHouseId();

		matInoutRepository.deleteById(mio_pk);
//		this.matInoutRepository.flush();

//		jdbcTemplate.query(
//				"SELECT sp_update_mat_in_house_by_inout(?, ?)",
//				rs -> {},  // 결과 무시
//				matPk, storeHouseId
//		);


		result.success = true;

		return result;
	}
	
	// 엑셀데이터 그리드로 변환
	@SuppressWarnings("unchecked")
	@GetMapping("/trans_multi_input_data")
	public AjaxResult transMultiInputData(
			@RequestParam MultiValueMap<String,Object> Q
			) throws JSONException, JsonMappingException, JsonProcessingException {
		
		AjaxResult result = new AjaxResult();
		
		List<Map<String, Object>> data = CommonUtil.loadJsonListMap(Q.getFirst("Q").toString());
		
		List<Map<String, Object>> items = new ArrayList<Map<String, Object>>();
				
		for(int i = 0; i < data.size(); i++) {
			if(data.get(i).get("mat_code").toString().isEmpty()) {
				continue;
			}
			JSONObject row = new JSONObject();
			Material m = this.materialRepository.findByCode(data.get(i).get("mat_code").toString());
			if (m != null) {
				row.put("mat_name", m.getName());
			}
			row.put("input_qty", data.get(i).get("input_qty").toString());
			row.put("mat_code", data.get(i).get("mat_code").toString());
			Map<String, Object> map = new ObjectMapper().readValue(row.toString(), Map.class) ;
			items.add(map);
			
		}
		result.data = items;
		return result;
	}
	
	@PostMapping("/save_multi_data")
	@Transactional
	public AjaxResult saveMultiData(
			@RequestParam("Company_id") String companyId,
			@RequestParam("InoutType") String inoutType,
			@RequestParam MultiValueMap<String,Object> Q,
			@RequestParam("StoreHouse_id") String storeHouseId,
			@RequestParam("type") String type,
			@RequestParam("spjangcd") String spjangcd,
			HttpServletRequest request,
			Authentication auth) {
		
		User user = (User)auth.getPrincipal();
		
		// 현재 일자
		LocalDate date = LocalDate.now();
		DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd");
		
		// 현재 시간
		LocalTime time = LocalTime.now();
		DateTimeFormatter timeFormat = DateTimeFormatter.ofPattern("HH:mm:ss");
		
		String state = "confirmed";
		String _status = "a";
		
		List<Map<String, Object>> data = CommonUtil.loadJsonListMap(Q.getFirst("Q").toString());
		
		AjaxResult result = new AjaxResult();
		
		result.success = false;
		for (int i=0; i < data.size(); i++) {
			if(data.get(i).get("mat_code").toString().isEmpty()) {
				continue;
			}
			
			Material m = this.materialRepository.findByCode(data.get(i).get("mat_code").toString());
			String testYn = m.getInTestYN() != null ? m.getInTestYN() : "";
			Integer matId = m.getId();
			Integer qty = Integer.parseInt(data.get(i).get("input_qty").toString());
			
			MaterialInout mi = new MaterialInout();
			mi.setMaterialId(matId);
			mi.setInoutDate(LocalDate.parse(date.format(dateFormat)));
			mi.setInoutTime(LocalTime.parse(time.format(timeFormat)));
			mi.setCompanyId(CommonUtil.tryIntNull(companyId));
			mi.setStoreHouseId(Integer.parseInt(storeHouseId));

			if (type.equals("in")) {
				mi.setInOut("in");
				mi.setInputType(inoutType);
				if(testYn.equals("Y")) {
					mi.setPotentialInputQty((float)qty);
					state = "waiting";
					_status = "t";
				} else {
					mi.setInputQty((float)qty);
					mi.setOutputQty(0f);
					mi.setOutputType("");
				}
			} else if(type.equals("recall")){
				mi.setInOut("recall");
				mi.setOutputType(inoutType);
				mi.setOutputQty((float)qty);
				mi.setInputQty(0f);
				mi.setInputType("");

			} else if(type.equals("return")){
				mi.setInOut("return");
				mi.setInputType(inoutType);
				mi.setInputQty((float)qty);
				mi.setOutputQty(0f);
				mi.setOutputType("");

			} else  {
				mi.setInOut("out");
				mi.setOutputType(inoutType);
				mi.setOutputQty((float)qty);
				mi.setInputQty(0f);
				mi.setInputType("");
			}
			mi.setState(state);
			mi.set_status(_status);
			mi.set_audit(user);
			mi.setSpjangcd(spjangcd);
			this.matInoutRepository.save(mi);
			
		}
		result.success = true;
		
		return result;
	}
	
	@GetMapping("/mio_lot_list")
	public AjaxResult mioLotList(
			@RequestParam("mio_id") String mioId) {
		
		List<Map<String, Object>> items = this.lotService.mioLotList(mioId);
		AjaxResult result = new AjaxResult();
		result.data = items;
		return result;
	}
	
	@GetMapping("/mio_test_list")
	public AjaxResult mioTestList(
			@RequestParam("mio_id") Integer mioId) {
		
		
		List<TestResult> trList = this.testResultRepository.findBySourceTableNameAndSourceDataPk("mat_inout", mioId);
		
		List<Map<String, Object>> items = null;
		Integer testMasterId = null;
		
		if (!trList.isEmpty()) {
			items = this.materialInoutService.mioTestList(mioId,trList.get(0).getId());
		} else {
			testMasterId = this.materialInoutService.getTestMasterByItem(mioId);

			if (testMasterId != null) {
				items = this.materialInoutService.prodTestListByTestMaster(testMasterId);
			} else{
				items = this.materialInoutService.mioTestDefaultList();
			}

		}
		
		Map<String, Object> effectDt = this.materialInoutService.getEffectDate(mioId);
		
		String effDt = effectDt.get("EffectiveDate") != null ? effectDt.get("EffectiveDate").toString() : null;
		
		
		Map<String, Object> item = new HashMap<>();
		
		item.put("EffectiveDate", effDt);
		item.put("testDate", items.get(0).get("testDate"));
		item.put("CheckName", items.get(0).get("CheckName"));
		item.put("JudgeCode", items.get(0).get("JudgeCode"));
		item.put("CharResult", items.get(0).get("CharResult"));
		item.put("testMasterId", items.get(0).get("testMasterId"));
		item.put("testResultId", items.get(0).get("testResultId"));
		item.put("mioList", items);
		
		AjaxResult result = new AjaxResult();
		result.data = item;
		return result;
	}
	
	@PostMapping("/lot_save")
	@Transactional
	public AjaxResult lotSave(
			@RequestBody MultiValueMap<String,Object> Q,
			@RequestParam("Material_id") String materialId,
			@RequestParam("StoreHouse_id") Integer storeHouseId,
			@RequestParam("mio_id") String mioId,
			@RequestParam("spjangcd") String spjangcd,
			HttpServletRequest request,
			Authentication auth) {
		
		User user = (User)auth.getPrincipal();
		
		AjaxResult result = new AjaxResult();
		
		Timestamp today = new Timestamp(System.currentTimeMillis());
		
		List<Map<String, Object>> data = CommonUtil.loadJsonListMap(Q.getFirst("Q").toString());
		
		result.success = false;
		for (int i=0; i < data.size(); i++) {
			MaterialLot ml = null;
			String LotNumber = null;
			if (!data.get(i).get("LotNumber").toString().isEmpty()) {
				LotNumber = data.get(i).get("LotNumber").toString();
				ml = this.matLotRepository.getByLotNumber(LotNumber);
				if (data.get(i).get("Description") != null) {
					ml.setDescription(data.get(i).get("Description").toString());
				}
				ml.setSpjangcd(spjangcd);
				this.matLotRepository.save(ml);
			} else {
				LotNumber = this.lotService.make_lot_in_number();
				String effectiveDate = data.get(i).get("EffectiveDate").toString() + " 00:00:00";
				ml = new MaterialLot();
				ml.setLotNumber(LotNumber);
				ml.setMaterialId(Integer.parseInt(materialId));
				ml.setInputQty(Float.parseFloat(data.get(i).get("InputQty").toString()));
				ml.setCurrentStock(Float.parseFloat(data.get(i).get("InputQty").toString()));
				ml.setInputDateTime(today);
				ml.setEffectiveDate(Timestamp.valueOf(effectiveDate));
				ml.setSourceTableName("mat_inout");
				ml.setSourceDataPk(Integer.parseInt(mioId));
				if (data.get(i).get("Description") != null) {
					ml.setDescription(data.get(i).get("Description").toString());
				}
				ml.setStoreHouseId(storeHouseId);
				ml.set_audit(user);
				ml.setSpjangcd(spjangcd);
				ml = this.matLotRepository.save(ml);
			}
			
			result.success = true;
		}
		
		return result;
	}
	
	@PostMapping("/test_save")
	@Transactional
	public AjaxResult testSave(
			@RequestBody MultiValueMap<String,Object> Q,
			@RequestParam(value = "material_id", required = false) Integer materialId,
			@RequestParam(value = "testRemark", required = false) String testRemark,
			@RequestParam(value = "test_mast_id", required = false) String testMastId,
			@RequestParam(value = "test_result_id", required = false) String testResultId,
			@RequestParam(value = "judg_grp", required = false) String judgGrp,
			@RequestParam(value = "test_date", required = false) String test_date,
			@RequestParam(value = "effective_date", required = false) String effectiveDate,
			@RequestParam(value = "mio_id", required = false) Integer mioId,
			@RequestParam("spjangcd") String spjangcd,
			HttpServletRequest request,
			Authentication auth) {
		
		User user = (User)auth.getPrincipal();
		
		AjaxResult result = new AjaxResult();
		
		Timestamp testDate = Timestamp.valueOf(test_date+ " 00:00:00");
		
		if (StringUtils.hasText(testResultId)) {
			List<TestItemResult> trList = this.testItemResultRepository.findByTestResultId(Integer.parseInt(testResultId));
			
			// 결과 삭제
			if(trList.size() > 0) {
				for (int i = 0; i < trList.size(); i++) {
					this.testItemResultRepository.deleteById(trList.get(i).getId());
				}
			}
			
			this.testItemResultRepository.flush();
		}
		
		
		TestResult tr = new TestResult();
		
		if (StringUtils.hasText(testResultId)) {
			tr = this.testResultRepository.getTestResultById(Integer.parseInt(testResultId));
		} else {
			tr.setSourceDataPk(mioId);
			tr.setSourceTableName("mat_inout");
			tr.setMaterialId(materialId);
		}
		
		tr.setTestMasterId(Integer.parseInt(testMastId));
		tr.setTestDateTime(testDate);
		tr.set_audit(user);
		tr.setSpjangcd(spjangcd);
		
		this.testResultRepository.saveAndFlush(tr);
		
		
		List<Map<String, Object>> data = CommonUtil.loadJsonListMap(Q.getFirst("Q").toString());
		
		for(int i = 0; i < data.size(); i++) {
			TestItemResult tir = new TestItemResult();
			tir.setJudgeCode(judgGrp);
			tir.setTestDateTime(testDate);
			tir.setSampleID(String.valueOf(materialId) + "/" +mioId);
			tir.setCharResult(testRemark);
			tir.setTestItemId(Integer.parseInt(data.get(i).get("id").toString()));
			tir.setTestResultId(tr.getId());
			
			if(data.get(i).get("result1") != null) {
				tir.setChar1(data.get(i).get("result1").toString());
			}
			tir.set_audit(user);
			tir.setSpjangcd(spjangcd);
			this.testItemResultRepository.save(tir);
		}
		
		MaterialInout mi = this.matInoutRepository.getMatInoutById(mioId);
		// 유효기간 변경
		if(StringUtils.hasText(effectiveDate)) {
			Timestamp effDt = Timestamp.valueOf(effectiveDate+ " 00:00:00");
			mi.setEffectiveDate(effDt);
		}

		mi.setState("confirmed");
		if (!"부적합".equals(judgGrp)) {
			// 적합한 경우에만 입고 처리
			mi.setInputQty(mi.getPotentialInputQty());
			mi.setPotentialInputQty((float)0);

			// 트리거 작동용 상태 변경
			mi.set_status("a");
		}

		this.matInoutRepository.save(mi);
		
		Map<String, Object> item = new HashMap<>();
		item.put("id", mioId);
		
		result.data = item;
		
		return result;
	}
	
	@PostMapping("/check_in_test")
	@Transactional
	public AjaxResult checkInTest(
			@RequestBody MultiValueMap<String,Object> Q,
			HttpServletRequest request,
			Authentication auth) {
		
		User user = (User)auth.getPrincipal();
		
		AjaxResult result = new AjaxResult();
		
		List<Map<String, Object>> data = CommonUtil.loadJsonListMap(Q.getFirst("Q").toString());
	
		for(int i = 0; i < data.size(); i++) {
			Integer id = Integer.parseInt(data.get(i).get("id").toString());
			Float inputQty = Float.parseFloat(data.get(i).get("PotentialInputQty").toString());
			MaterialInout mi = this.matInoutRepository.getMatInoutById(id);
			mi.setInputQty(inputQty);
			mi.setPotentialInputQty((float)0);
			mi.setState("confirmed");
			mi.set_status("a");
			mi.set_audit(user);
			this.matInoutRepository.save(mi);
		}
		
		return result;
	}

	@GetMapping("/read_balju")
	public AjaxResult getbaljuList(
			@RequestParam(value="start", required=false) String start_date,
			@RequestParam(value="end", required=false) String end_date,
			@RequestParam("spjangcd") String spjangcd,
			HttpServletRequest request) {

		start_date = start_date + " 00:00:00";
		end_date = end_date + " 23:59:59";

		Timestamp start = Timestamp.valueOf(start_date);
		Timestamp end = Timestamp.valueOf(end_date);

		List<Map<String, Object>> items = this.materialInoutService.getBaljuList(start, end, spjangcd);

		AjaxResult result = new AjaxResult();
		result.data = items;

		return result;
	}

	@GetMapping("/read_balju_in")
	public AjaxResult getbaljuInList(
			@RequestParam(value="start", required=false) String start_date,
			@RequestParam(value="end", required=false) String end_date,
			@RequestParam(value="cboCompanyHidden", required=false) Integer cboCompany,
			@RequestParam(value = "keyword", required=false) String keyword,
			@RequestParam("spjangcd") String spjangcd,
			HttpServletRequest request) {

		start_date = start_date + " 00:00:00";
		end_date = end_date + " 23:59:59";

		Timestamp start = Timestamp.valueOf(start_date);
		Timestamp end = Timestamp.valueOf(end_date);

		List<Map<String, Object>> items = this.materialInoutService.getBaljuInList(start, end, spjangcd, cboCompany, keyword);

		AjaxResult result = new AjaxResult();
		result.data = items;

		return result;
	}

	@PostMapping("/save_balju")
	@Transactional
	public AjaxResult saveBaljuInout(
			@RequestBody List<Map<String, Object>> baljuList,
			HttpServletRequest request,
			Authentication auth) {

		User user = (User)auth.getPrincipal();
		AjaxResult result = new AjaxResult();

		for (Map<String, Object> item : baljuList) {
			try {
				Integer bal_pk = (Integer) item.get("id");
				String description = (String) item.get("Description2");
				if (description == null || description.trim().isEmpty()) {
					description = "발주 입고";
				}
				String inoutQtyStr = String.valueOf(item.get("inputQty")); // '입고 수량'
				String materialIdStr = String.valueOf(item.get("Material_id"));
				String storeHouseIdStr = String.valueOf(item.get("StoreHouse_id"));

				Integer matPk = Integer.parseInt(materialIdStr);
				Integer qty = Integer.parseInt(inoutQtyStr);

				MaterialInout mi = new MaterialInout();
				mi.setInoutDate(LocalDate.now());
				mi.setInoutTime(LocalTime.now());
				mi.setMaterialId(matPk);
				mi.setStoreHouseId(Integer.parseInt(storeHouseIdStr));

				Material m = materialRepository.getMaterialById(matPk);
				String testYn = m.getInTestYN() != null ? m.getInTestYN() : "";

				if ("Y".equals(testYn)) {
					mi.setPotentialInputQty((float) qty);
					mi.setState("waiting");
					mi.set_status("t");
				} else {
					mi.setInputQty((float) qty);
					mi.setState("confirmed");
					mi.set_status("a");
				}

				mi.setDescription(description);
				mi.setInOut("in");
				mi.set_audit(user);
				mi.setSourceDataPk(bal_pk);
				mi.setSourceTableName("balju");
				mi.setSpjangcd((String) item.get("spjangcd"));
				mi.setCompanyId((Integer) item.get("Company_id"));

				Balju balju = this.bujuRepository.getBujuById(bal_pk);

				double sujuQty2 = jdbcTemplate.queryForObject("""
					SELECT COALESCE(SUM("InputQty"), 0)
					FROM mat_inout
					WHERE "SourceDataPk" = ? 
					  AND "SourceTableName" = 'balju'
					  AND COALESCE("_status", 'a') = 'a'
				""", Double.class, bal_pk);

				balju.setShipmentState(storeHouseIdStr);
				mi.setInputType("order_in");

				matInoutRepository.save(mi);
				bujuRepository.save(balju);

			} catch (Exception e) {
				result.success = false;
				result.message = "처리 중 오류 발생: " + e.getMessage();
				return result;
			}
		}
		result.success = true;

		return result;
	}

	@PostMapping("/force-complete")
	@Transactional
	public AjaxResult forceCompleteSuju(@RequestBody Map<String, Object> payload) {
		AjaxResult result = new AjaxResult();

		List<Integer> sujuPkList = (List<Integer>) payload.get("baljuPkList");
		bujuRepository.forceCompleteBaljuList(sujuPkList);
		return result;
	}

	@PostMapping("/save_balju_return")
	@Transactional
	public AjaxResult saveBaljuReturn(
			@RequestBody List<Map<String, Object>> baljuList,
			HttpServletRequest request,
			Authentication auth) {

		User user = (User)auth.getPrincipal();
		AjaxResult result = new AjaxResult();

		for (Map<String, Object> item : baljuList) {
			try {
				Integer bal_pk = (Integer) item.get("id");
				String description = (String) item.get("Description2");
				if (description == null || description.trim().isEmpty()) {
					description = "발주 반품";
				}
				String inoutQtyStr = String.valueOf(item.get("returnQty")); // '반품 수량'
				String materialIdStr = String.valueOf(item.get("Material_id"));
				String storeHouseIdStr = String.valueOf(item.get("StoreHouse_id"));

				Integer matPk = Integer.parseInt(materialIdStr);
				Integer qty = Integer.parseInt(inoutQtyStr);

				MaterialInout mi = new MaterialInout();
				mi.setInoutDate(LocalDate.now());
				mi.setInoutTime(LocalTime.now());
				mi.setMaterialId(matPk);
				mi.setStoreHouseId(Integer.parseInt(storeHouseIdStr));

				mi.setInputQty((float) qty);
				mi.setState("confirmed");
				mi.set_status("a");
				mi.setDescription(description);
				mi.setInOut("return");
				mi.set_audit(user);
				mi.setSourceDataPk(bal_pk);
				mi.setSourceTableName("balju");
				mi.setSpjangcd((String) item.get("spjangcd"));
				mi.setCompanyId((Integer) item.get("Company_id"));

				Balju balju = this.bujuRepository.getBujuById(bal_pk);

				double sujuQty2 = jdbcTemplate.queryForObject("""
					SELECT COALESCE(SUM("InputQty"), 0)
					FROM mat_inout
					WHERE "SourceDataPk" = ? 
					  AND "SourceTableName" = 'balju'
					  AND COALESCE("_status", 'a') = 'a'
				""", Double.class, bal_pk);

				balju.setShipmentState(storeHouseIdStr);
				mi.setInputType("balju_return");

				matInoutRepository.save(mi);
				bujuRepository.save(balju);

			} catch (Exception e) {
				result.success = false;
				result.message = "처리 중 오류 발생: " + e.getMessage();
				return result;
			}
		}
		result.success = true;

		return result;
	}
	
}
