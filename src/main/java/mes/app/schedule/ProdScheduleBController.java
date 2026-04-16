package mes.app.schedule;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.transaction.Transactional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import mes.app.schedule.service.ProdScheduleBService;
import mes.domain.entity.BundleHead;
import mes.domain.entity.MatRequ;
import mes.domain.entity.Suju;
import mes.domain.entity.SystemCode;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.BundleHeadRepository;
import mes.domain.repository.MatRequRepository;
import mes.domain.repository.SujuRepository;
import mes.domain.repository.SysCodeRepository;
import mes.domain.services.CommonUtil;
import mes.domain.services.DateUtil;

@RestController
@RequestMapping("/api/schedule/prod_schedule_b")
public class ProdScheduleBController {

	@Autowired
	private ProdScheduleBService prodScheduleBService;

	@Autowired
	BundleHeadRepository bundleHeadRepository;

	@Autowired
	SujuRepository sujuRepository;

	@Autowired
	MatRequRepository matRequRepository;

	@Autowired
	SysCodeRepository sysCodeRepository;
	
	// 소요량산출 헤드리스트 (생산계획 리스트)
	@GetMapping("/bundle_head_list")
	public AjaxResult getBundleHeadList(
			@RequestParam(value="date_from", required=false) String date_from, 
			@RequestParam(value="date_to", required=false) String date_to,
			HttpServletRequest request) {

		Timestamp start = Timestamp.valueOf(date_from + " 00:00:00");
        Timestamp end = Timestamp.valueOf(date_to + " 23:59:59");
        
		List<Map<String, Object>> items = this.prodScheduleBService.getBundleHeadList(start, end);
        AjaxResult result = new AjaxResult();
        result.data = items;
		return result;
	}

	// not bundled suju list (수주추가탭)
	@GetMapping("/not_counted_suju_search")
	public AjaxResult getNotCountedSujuList(
			@RequestParam(value="plan_from", required=false) String plan_from, 
			@RequestParam(value="plan_to", required=false) String plan_to,
			HttpServletRequest request) {

		Timestamp start = Timestamp.valueOf(plan_from + " 00:00:00");
        Timestamp end = Timestamp.valueOf(plan_to + " 23:59:59");
        
		List<Map<String, Object>> items = this.prodScheduleBService.getNotCountedSujuList(start, end);
        AjaxResult result = new AjaxResult();
        result.data = items;
		return result;
	}

	// bundled suju list (수주내역탭)	
	@GetMapping("/bundled_suju_list")
	public AjaxResult getBundledSujuList(
			@RequestParam(value="head_id", required=false) Integer head_id, 
			HttpServletRequest request) {
        
		List<Map<String, Object>> items = this.prodScheduleBService.getBundledSujuList(head_id);
        AjaxResult result = new AjaxResult();
        result.data = items;
		return result;
	}

	// 제품필요량 조회 (제품필요량탭)
	@GetMapping("/product_required_list")
	public AjaxResult getProductRequiredList(
			@RequestParam(value="head_id", required=false) Integer head_id, 
			HttpServletRequest request) {
		
		List<Map<String, Object>> items = this.prodScheduleBService.getMaterialRequirementList("bundle_head", head_id, "product");
        AjaxResult result = new AjaxResult();
        result.data = items;
		return result;
	}

	// 반제품소요량조회 (반제품소요량탭)
	@GetMapping("/semiprod_required_list")
	public AjaxResult getSemiprodRequiredList(
			@RequestParam(value="head_id", required=false) Integer head_id, 
			HttpServletRequest request) {
        
		List<Map<String, Object>> items = this.prodScheduleBService.getMaterialRequirementList("bundle_head", head_id, "semi");
        AjaxResult result = new AjaxResult();
        result.data = items;
		return result;
	}

	// 원부자재 소요량 조회 (원부자재소요량탭)
	@GetMapping("/rawmat_required_list")
	public AjaxResult getRawmatRequiredList(
			@RequestParam(value="head_id", required=false) Integer head_id, 
			HttpServletRequest request) {
        
		List<Map<String, Object>> items = this.prodScheduleBService.getMaterialRequirementList("bundle_head", head_id, "material");
        AjaxResult result = new AjaxResult();
        result.data = items;
		return result;
	}

	// 1. 수주추가탭
	// 새 생산계획 저장
	@PostMapping("/create_bundle")
	@Transactional
	public AjaxResult createBundle(
			@RequestBody MultiValueMap<String,Object> Q,
			HttpServletRequest request,
			Authentication auth) {

		User user = (User)auth.getPrincipal();
		
		AjaxResult result = new AjaxResult();
		
		BundleHead bundleHead = new BundleHead();
		bundleHead.setTableName("suju");
		bundleHead.set_status("none");
		bundleHead.set_audit(user);
		bundleHead = this.bundleHeadRepository.save(bundleHead);

		Integer head_id = bundleHead.getId();
		saveSujuData(head_id, Q);
		
		result.data = bundleHead;
		
		return result;
	}
	
	// 생산계획에 추가
	@PostMapping("/add_bundle_suju")
	@Transactional
	public AjaxResult addBundleSuju(
			@RequestParam(value = "head_id", required = false) Integer head_id,
			@RequestBody MultiValueMap<String,Object> Q,
			HttpServletRequest request,
			Authentication auth) {
		
		AjaxResult result = new AjaxResult();
		saveSujuData(head_id, Q);
		return result;
	}
	
	// 수주내역 저장
	private void saveSujuData(Integer head_id, MultiValueMap<String,Object> Q) {

		List<Integer> items = loadJsonList(Q.getFirst("Q").toString());		

		for (int i = 0; i < items.size(); i++) {		
			Integer id = items.get(i);
			Suju suju = this.sujuRepository.getSujuById(id);
			
			suju.setPlanDataPk(head_id);
			suju.setPlanTableName("bundle_head");

			this.sujuRepository.save(suju);
		}
	}	
	
	// 2. 수주내역탭
	// 제품 필요량 집계
	@PostMapping("/save_product_required_sum")
	@Transactional
	public AjaxResult saveProductRequiredSum(
			@RequestParam(value = "head_id", required = false) Integer head_id,
			HttpServletRequest request,
			Authentication auth) {
		
		AjaxResult result = new AjaxResult();

		User user = (User)auth.getPrincipal();
		
		// 제품 필요량 집계
		boolean flag = this.prodScheduleBService.saveProdRequCalc("bundle_head", head_id, "product", user.getId());
		result.success = flag;
		
		// 생산계획구간 상태 변경
		result.data = updateBundleHeadSatus(head_id, "planned", user);
				
		return result;
	}
	
	// 3. 제품필요량탭
	// 재고예약
	@PostMapping("/product_reservation")
	@Transactional
	public AjaxResult productReservation(
			@RequestBody MultiValueMap<String,Object> Q,
			HttpServletRequest request,
			Authentication auth) {
		
		AjaxResult result = new AjaxResult();

		User user = (User)auth.getPrincipal();
		
	    List<Map<String, Object>> items = CommonUtil.loadJsonListMap(Q.getFirst("Q").toString());

		if (items.size() == 0) {
			result.success = false;
			return result;
		}

		for (int i = 0; i < items.size(); i++) {
			Integer id = (Integer) items.get(i).get("id");
            //Float reservationStock_input = Float.valueOf((String)items.get(i).get("ReservationStock_input"));
			Float reservationStock_input = Float.valueOf(String.valueOf(items.get(i).get("ReservationStock_input")));
			
            MatRequ matRequ = this.matRequRepository.getMatRequById(id);
            
            if (matRequ != null) {
            	matRequ.setReservationStock(reservationStock_input);
            	matRequ.set_audit(user);
            	this.matRequRepository.save(matRequ);         	
            } else {
            	result.success = false;
            }
		}
		
		return result;
	}
	
	// 생산필요량저장
	@PostMapping("/production_qty_save")
	@Transactional
	public AjaxResult productionQtySave(
			@RequestBody MultiValueMap<String,Object> Q,
			HttpServletRequest request,
			Authentication auth) {
		
		AjaxResult result = new AjaxResult();

		User user = (User)auth.getPrincipal();
		
	    List<Map<String, Object>> items = CommonUtil.loadJsonListMap(Q.getFirst("Q").toString());

		if (items.size() == 0) {
			result.success = false;
			return result;
		}
		
		for (int i = 0; i < items.size(); i++) {
			Integer id = (Integer) items.get(i).get("id");
            //Float requestQty = Float.valueOf((String)items.get(i).get("RequestQty"));	// 입력 받은 생산 필요량
			Float requestQty = Float.valueOf(String.valueOf(items.get(i).get("RequestQty")));	// 입력 받은 생산 필요량
            
            MatRequ matRequ = this.matRequRepository.getMatRequById(id);
            
            if (matRequ != null) {
            	matRequ.setRequestQty(requestQty);
            	matRequ.set_audit(user);
            	this.matRequRepository.save(matRequ);         	
            } else {
            	result.success = false;
            }
		}
		
		return result;
	}
	
	// 생산필요량확정
	@PostMapping("/production_qty_confirm")
	@Transactional
	public AjaxResult productionQtyConfirm(
			@RequestParam(value = "head_id", required = false) Integer head_id,
			@RequestBody MultiValueMap<String,Object> Q,
			HttpServletRequest request,
			Authentication auth) {
		
		AjaxResult result = new AjaxResult();

		User user = (User)auth.getPrincipal();

		// 생산계획구간 상태 제품확정으로 변경
	    updateBundleHeadSatus(head_id, "product", user);
		
		String stateName = "";
		SystemCode systemCode = sysCodeRepository.getSysCodeByCodeTypeAndCode("prod_week_term_state", "product");

		if (systemCode != null) {
			stateName = systemCode.getValue();
		}

		result.StateName = stateName;
		return result;
	}
	
	// 4. 반제품소요량탭
	// 소요량산출
	@PostMapping("/semiprod_required_save")
	@Transactional
	public AjaxResult semiprodRequiredSave(
			@RequestParam(value = "head_id", required = false) Integer head_id,
			HttpServletRequest request,
			Authentication auth) {
		
		AjaxResult result = new AjaxResult();

		User user = (User)auth.getPrincipal();
		
		// 제품 필요량 집계
		this.prodScheduleBService.saveMatRequCalc("bundle_head", head_id, "semi", user.getId());
				
		return result;
	}
	
	// 재고예약
	@PostMapping("/semi_product_reservation")
	@Transactional
	public AjaxResult semiProductReservation(
			@RequestParam(value = "head_id", required = false) Integer head_id,
			@RequestBody MultiValueMap<String,Object> Q,
			HttpServletRequest request,
			Authentication auth) {

		AjaxResult result = new AjaxResult();

		User user = (User)auth.getPrincipal();
		
	    List<Map<String, Object>> items = CommonUtil.loadJsonListMap(Q.getFirst("Q").toString());

		if (items.size() == 0) {
			result.success = false;
			return result;
		}

		for (int i = 0; i < items.size(); i++) {
			Integer id = (Integer) items.get(i).get("id");
            //Float ReservationStock_input = Float.valueOf((String)items.get(i).get("ReservationStock_input"));
			Float reservationStock_input = Float.valueOf(String.valueOf(items.get(i).get("ReservationStock_input")));
            	            
            MatRequ matRequ = this.matRequRepository.getMatRequById(id);
            
            if (matRequ != null) {
            	matRequ.setReservationStock(reservationStock_input);
            	matRequ.set_audit(user);
            	this.matRequRepository.save(matRequ);         	
            } else {
            	result.success = false;
            }
		}
		
		return result;
	}
	
	// 반제품필요량저장
	@PostMapping("/semiprod_qty_save")
	@Transactional
	public AjaxResult semiprodQtySave(
			@RequestParam(value = "head_id", required = false) Integer head_id,
			@RequestBody MultiValueMap<String,Object> Q,
			HttpServletRequest request,
			Authentication auth) {

		AjaxResult result = new AjaxResult();

		User user = (User)auth.getPrincipal();
		
	    List<Map<String, Object>> items = CommonUtil.loadJsonListMap(Q.getFirst("Q").toString());

		if (items.size() == 0) {
			result.success = false;
			return result;
		}
		
		for (int i = 0; i < items.size(); i++) {
			Integer id = (Integer) items.get(i).get("id");
            //Float requestQty = Float.valueOf((String)items.get(i).get("RequestQty"));	// 입력 받은 생산 필요량
			Float requestQty = Float.valueOf(String.valueOf(items.get(i).get("RequestQty")));	// 입력 받은 생산 필요량
            
            MatRequ matRequ = this.matRequRepository.getMatRequById(id);
            
            if (matRequ != null) {
            	matRequ.setRequestQty(requestQty);
            	matRequ.set_audit(user);
            	matRequ = this.matRequRepository.save(matRequ); 
    			result.data = matRequ;        	
            } else {
            	result.success = false;
            }
		}
		
		return result;
	}
	
	// 반제품필요량확정
	@PostMapping("/semiprod_qty_confirm")
	@Transactional
	public AjaxResult semiprodQtyConfirm(
			@RequestParam(value = "head_id", required = false) Integer head_id,
			@RequestBody MultiValueMap<String,Object> Q,
			HttpServletRequest request,
			Authentication auth) {
		
		AjaxResult result = new AjaxResult();

		User user = (User)auth.getPrincipal();

		// 생산계획구간 상태 제품확정으로 변경
	    updateBundleHeadSatus(head_id, "semi", user);
		
		String stateName = "";
		SystemCode systemCode = sysCodeRepository.getSysCodeByCodeTypeAndCode("prod_week_term_state", "semi");

		if (systemCode != null) {
			stateName = systemCode.getValue();
		}

		result.StateName = stateName;
		return result;
	}

	// 5. 원부자재소요량탭
	// 소요량산출
	@PostMapping("/rawmat_required_save")
	@Transactional
	public AjaxResult rawmatRequiredSave(
			@RequestParam(value = "head_id", required = false) Integer head_id,
			HttpServletRequest request,
			Authentication auth) {
		
		AjaxResult result = new AjaxResult();

		User user = (User)auth.getPrincipal();
		
		// 제품 필요량 집계
		this.prodScheduleBService.saveMatRequCalc("bundle_head", head_id, "material", user.getId());
				
		return result;
	}
	
	// 재고예약
	@PostMapping("/rawmat_reservation")
	@Transactional
	public AjaxResult rawmatReservation(
			@RequestParam(value = "head_id", required = false) Integer head_id,
			@RequestBody MultiValueMap<String,Object> Q,
			HttpServletRequest request,
			Authentication auth) {

		AjaxResult result = new AjaxResult();

		User user = (User)auth.getPrincipal();
		
	    List<Map<String, Object>> items = CommonUtil.loadJsonListMap(Q.getFirst("Q").toString());

		if (items.size() == 0) {
			result.success = false;
			return result;
		}

		for (int i = 0; i < items.size(); i++) {
			Integer id = (Integer) items.get(i).get("id");			
            //Float ReservationStock_input = Float.valueOf((String)items.get(i).get("ReservationStock_input"));
			Float reservationStock_input = Float.valueOf(String.valueOf(items.get(i).get("ReservationStock_input")));
            	            
            MatRequ matRequ = this.matRequRepository.getMatRequById(id);
            
            if (matRequ != null) {
            	matRequ.setReservationStock(reservationStock_input);
            	matRequ.set_audit(user);
            	this.matRequRepository.save(matRequ);         	
            } else {
            	result.success = false;
            }
		}
		
		return result;
	}
	
	// 발주 요청량 저장
	@PostMapping("/rawmat_qty_purchase_save")
	@Transactional
	public AjaxResult rawmatQtyPurchaseSave(
			@RequestParam(value = "head_id", required = false) Integer head_id,
			@RequestBody MultiValueMap<String,Object> Q,
			HttpServletRequest request,
			Authentication auth) {

		AjaxResult result = new AjaxResult();

		User user = (User)auth.getPrincipal();
		
	    List<Map<String, Object>> items = CommonUtil.loadJsonListMap(Q.getFirst("Q").toString());

		if (items.size() == 0) {
			result.success = false;
			return result;
		}

        Timestamp today = DateUtil.getNowTimeStamp();
        
		for (int i = 0; i < items.size(); i++) {
			Integer id = (Integer) items.get(i).get("id");
            //Float requestQty = Float.valueOf((String)items.get(i).get("RequestQty"));	// 입력 받은 생산 필요량
			Float requestQty = Float.valueOf(String.valueOf(items.get(i).get("RequestQty")));	// 입력 받은 생산 필요량
            
            MatRequ matRequ = this.matRequRepository.getMatRequById(id);
            
            if (matRequ != null) {
            	matRequ.setRequestQty(requestQty);
            	matRequ.setRequestDate(today);
            	matRequ.set_audit(user);
            	matRequ = this.matRequRepository.save(matRequ); 
    			result.data = matRequ;        	
            } else {
            	result.success = false;
            }
		}
		
		return result;
	}
	
	// 발주 요청량 확정
	@PostMapping("/rawmat_qty_purchase_confirm")
	@Transactional
	public AjaxResult rawmatQtyPurchaseConfirm(
			@RequestParam(value = "head_id", required = false) Integer head_id,
			@RequestBody MultiValueMap<String,Object> Q,
			HttpServletRequest request,
			Authentication auth) {
		
		AjaxResult result = new AjaxResult();

		User user = (User)auth.getPrincipal();

		// 생산계획구간 상태 제품확정으로 변경
	    updateBundleHeadSatus(head_id, "material", user);
		
		String stateName = "";
		SystemCode systemCode = sysCodeRepository.getSysCodeByCodeTypeAndCode("prod_week_term_state", "material");

		if (systemCode != null) {
			stateName = systemCode.getValue();
		}

		result.StateName = stateName;
		return result;
	}
		
	// 생산계획구간 상태 업데이트
	private BundleHead updateBundleHeadSatus(Integer head_id, String status, User user) {
		
		BundleHead bundleHead = this.bundleHeadRepository.getBundleHeadById(head_id);
		bundleHead.set_status(status);
		bundleHead.set_audit(user);
		
		bundleHead = this.bundleHeadRepository.save(bundleHead);
		
		return bundleHead;
	}
	
	private static List<Integer> loadJsonList(String strJson) {
		
		ObjectMapper objectMapper = new ObjectMapper();
		List<Integer> result = null;
		try {
			result = objectMapper.readValue(strJson, new TypeReference<List<Integer>>(){});
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
		
		return result;
	}
}
