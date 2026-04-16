package mes.app.schedule;

import java.sql.Timestamp;
import java.util.ArrayList;
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

import mes.app.schedule.service.ProdScheduleAService;
import mes.domain.entity.MatRequ;
import mes.domain.entity.ProdWeekTerm;
import mes.domain.entity.Suju;
import mes.domain.entity.SystemCode;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.MatReqRepository;
import mes.domain.repository.ProdWeekTermRepository;
import mes.domain.repository.SujuRepository;
import mes.domain.repository.SysCodeRepository;
import mes.domain.services.CommonUtil;

@RestController
@RequestMapping("/api/schedule/prod_schedule_a")
public class ProdScheduleAController {
	
	@Autowired
	private ProdScheduleAService prodScheduleAService;
	
	@Autowired
	MatReqRepository matReqRepository;
	
	@Autowired
	SujuRepository sujuRepository;
	
	@Autowired
	ProdWeekTermRepository prodWeekTermRepository;
	
	@Autowired
	SysCodeRepository sysCodeRepository;
	// 생산계획구간조회
	@GetMapping("/prod_schedule_list")
	public AjaxResult getProdScheduleList(
			@RequestParam("year") String year,
			@RequestParam("month") String month) {
		
		List<Map<String, Object>> items = this.prodScheduleAService.getProdScheduleList(year,month);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	
	// 수주내역 조회
	@GetMapping("/suju_list")
	public AjaxResult getSujuList(
			@RequestParam("pwt_id") String pwtId) {
		
		List<Map<String, Object>> items = this.prodScheduleAService.getSujuList(pwtId);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	
	// 제품필요량 목록 조회
	@GetMapping("/prod_required_list")
	public AjaxResult getProdRequiredList(
			@RequestParam("pwt_id") String pwtId) {
		
		List<Map<String, Object>> items = this.prodScheduleAService.getMaterialRequirementList("prod_week_term", pwtId, "product");
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	
	// 반제품소요량 목록 조회
	@GetMapping("/semiprod_required_list")
	public AjaxResult getSemiprodRequiredList(
			@RequestParam("pwt_id") String pwtId) {
		
		List<Map<String, Object>> items = this.prodScheduleAService.getMaterialRequirementList("prod_week_term", pwtId, "semi");
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	
	// 원부자재 소요량 목록 조회
	@GetMapping("/rawmat_required_list")
	public AjaxResult getRawmatRequiredList(
			@RequestParam("pwt_id") String pwtId) {
		
		List<Map<String, Object>> items = this.prodScheduleAService.getMaterialRequirementList("prod_week_term", pwtId, "material");
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	
	//제품필요량 집계 저장
	@PostMapping("/prod_required_save")
	@Transactional
	public AjaxResult ProdRequiredSave(
			@RequestParam("pwt_id") String pwtId,
			@RequestBody MultiValueMap<String,Object> Q,
			HttpServletRequest request,
			Authentication auth) {
		
		User user = (User)auth.getPrincipal();
		
		AjaxResult result = new AjaxResult();
		
		List<Map<String, Object>> data = CommonUtil.loadJsonListMap(Q.getFirst("Q").toString());
		
		List<String> arrSuju = new ArrayList<String>();
		
		for(int i = 0; i < data.size(); i++) {
			String sujuId = data.get(i).get("suju_id").toString();
			arrSuju.add(sujuId);

		}
		
		for (int j = 0; j < arrSuju.size(); j++) {
			Suju sj = this.sujuRepository.getSujuById(Integer.parseInt(arrSuju.get(j).toString()));
			sj.set_audit(user);
			sj.setPlanTableName("prod_week_term");
			sj.setPlanDataPk(Integer.parseInt(pwtId));
		}
		
		List<MatRequ> mrList = this.matReqRepository.findBySourceTableNameAndSourceDataPkAndMaterialType("prod_week_term",Integer.parseInt(pwtId), "product");
		
		for(int k = 0; k < mrList.size(); k++) {
			this.matReqRepository.deleteById(mrList.get(k).getId());
		}
		this.matReqRepository.flush();
		boolean flag = this.prodScheduleAService.saveProdRequCalc("prod_week_term", pwtId,"product", user.getId());
		
		result.success = flag;
		
		return result;
		
	}
	
	// 제품재고예약
	@PostMapping("/product_reservation")
	public AjaxResult saveProductReservation(
			@RequestBody MultiValueMap<String,Object> Q,
			HttpServletRequest request,
			Authentication auth) {
		
		User user = (User)auth.getPrincipal();
		
		AjaxResult result = new AjaxResult();
		
		List<Map<String, Object>> data = CommonUtil.loadJsonListMap(Q.getFirst("Q").toString());
	
		for (int i=0; i<data.size(); i++) {
			Integer id = Integer.parseInt(data.get(i).get("id").toString());
			Integer reservationStockInput = Integer.parseInt(data.get(i).get("ReservationStock_input").toString());
			
			MatRequ mr = this.matReqRepository.getMatReqById(id);
			mr.setReservationStock((float)reservationStockInput);
			mr.set_audit(user);
			mr = this.matReqRepository.save(mr);
			result.data = mr;
		}
		
		return result;
	}
	
	// 제품필요량 저장
	@PostMapping("/production_qty_save")
	@Transactional
	public AjaxResult ProductionQtySave(
			@RequestBody MultiValueMap<String,Object> Q,
			HttpServletRequest request,
			Authentication auth) {
		
		User user = (User)auth.getPrincipal();
		
		AjaxResult result = new AjaxResult();
		
		List<Map<String, Object>> data = CommonUtil.loadJsonListMap(Q.getFirst("Q").toString());
	
		for (int i=0; i<data.size(); i++) {
			Integer id = Integer.parseInt(data.get(i).get("id").toString());
			Integer requestQty = Integer.parseInt(data.get(i).get("RequestQty").toString());
			
			MatRequ mr = this.matReqRepository.getMatReqById(id);
			mr.setRequestQty((float)requestQty);
			mr.set_audit(user);
			mr = this.matReqRepository.save(mr);
			result.data = mr;
		}
		
		return result;
	}
	
	// 제품필요량확정
	@PostMapping("/production_qty_confirm")
	@Transactional
	public AjaxResult ProductionQtyConfirm(
			@RequestParam("pwt_id") String pwtId,
			@RequestBody MultiValueMap<String,Object> Q,
			HttpServletRequest request,
			Authentication auth) {
		
		User user = (User)auth.getPrincipal();
		
		AjaxResult result = new AjaxResult();
	
		ProdWeekTerm pt = this.prodWeekTermRepository.getProdWeekTermById(Integer.parseInt(pwtId));
		
		pt.setState("product");
		pt.set_audit(user);
		
		String StateName = "";
		
		SystemCode queryset = this.sysCodeRepository.findByCodeTypeAndCode("prod_week_term_state","product");
		if (queryset != null) {
			StateName = queryset.getValue();
		}
		
		result.StateName = StateName;
		result.data = pt;
		
		return result;
	}
	// 반제품 소요량 산출
	@PostMapping("/semiprod_required_save")
	@Transactional
	public AjaxResult semiprodRequiredSave(
			@RequestParam("pwt_id") String pwtId,
			@RequestBody MultiValueMap<String,Object> Q,
			HttpServletRequest request,
			Authentication auth) {
		
		User user = (User)auth.getPrincipal();
		
		AjaxResult result = new AjaxResult();
		
		List<MatRequ> mrList = this.matReqRepository.findBySourceTableNameAndSourceDataPkAndMaterialType("prod_week_term",Integer.parseInt(pwtId), "semi");
		
		for(int k = 0; k < mrList.size(); k++) {
			this.matReqRepository.deleteById(mrList.get(k).getId());
		}
		
		this.matReqRepository.flush();
		boolean flag = this.prodScheduleAService.saveMatRequCalc("prod_week_term", pwtId,"semi", user.getId());
		
		result.success = flag;
		
		return result;
		
	}
	// 반제품 재고 예약
	@PostMapping("/semi_product_reservation")
	@Transactional
	public AjaxResult semiProductReservation(
			@RequestBody MultiValueMap<String,Object> Q,
			HttpServletRequest request,
			Authentication auth) {
		
		User user = (User)auth.getPrincipal();
		
		AjaxResult result = new AjaxResult();
		
		List<Map<String, Object>> data = CommonUtil.loadJsonListMap(Q.getFirst("Q").toString());
	
		for (int i=0; i<data.size(); i++) {
			Integer id = Integer.parseInt(data.get(i).get("id").toString());
			Integer reservationStockInput = Integer.parseInt(data.get(i).get("ReservationStock_input").toString());
			
			MatRequ mr = this.matReqRepository.getMatReqById(id);
			mr.setReservationStock((float)reservationStockInput);
			mr.set_audit(user);
			mr = this.matReqRepository.save(mr);
			result.data = mr;
		}
		
		return result;
	}
	// 반제품 필요량 저장
	@PostMapping("/semiprod_qty_save")
	@Transactional
	public AjaxResult semiprodQtySave(
			@RequestParam("pwt_id") String pwtId,
			@RequestBody MultiValueMap<String,Object> Q,
			HttpServletRequest request,
			Authentication auth) {
		
		User user = (User)auth.getPrincipal();
		
		AjaxResult result = new AjaxResult();
		
		List<Map<String, Object>> data = CommonUtil.loadJsonListMap(Q.getFirst("Q").toString());
	
		for (int i=0; i<data.size(); i++) {
			Integer id = Integer.parseInt(data.get(i).get("id").toString());
			Integer requestQty = Integer.parseInt(data.get(i).get("RequestQty").toString());
			
			MatRequ mr = this.matReqRepository.getMatReqById(id);
			mr.setRequestQty((float)requestQty);
			mr.set_audit(user);
			mr = this.matReqRepository.save(mr);
			result.data = mr;
		}
		
		return result;
	}
	
	// 반제품 생산필요량 확정
	@PostMapping("/semiprod_qty_confirm")
	@Transactional
	public AjaxResult semiprodQtyConfirm(
			@RequestParam("pwt_id") String pwtId,
			@RequestBody MultiValueMap<String,Object> Q,
			HttpServletRequest request,
			Authentication auth) {
		
		User user = (User)auth.getPrincipal();
		
		AjaxResult result = new AjaxResult();
		
		ProdWeekTerm pt = this.prodWeekTermRepository.getProdWeekTermById(Integer.parseInt(pwtId));
		
		pt.setState("semi");
		pt.set_audit(user);
		
		String StateName = "";
		SystemCode queryset = this.sysCodeRepository.findByCodeTypeAndCode("prod_week_term_state","semi");
		if (queryset != null) {
			StateName = queryset.getValue();
		}
		
		result.StateName = StateName;
		result.data = pt;
		
		return result;
	}
	
	// 원부자재 소요량 산출
	@PostMapping("/rawmat_required_save")
	@Transactional
	public AjaxResult rawmatRequiredSave(
			@RequestParam("pwt_id") String pwtId,
			@RequestBody MultiValueMap<String,Object> Q,
			HttpServletRequest request,
			Authentication auth) {
		
		User user = (User)auth.getPrincipal();
		
		AjaxResult result = new AjaxResult();
		
		List<MatRequ> mrList = this.matReqRepository.findBySourceTableNameAndSourceDataPkAndMaterialType("prod_week_term",Integer.parseInt(pwtId), "material");
		
		for(int k = 0; k < mrList.size(); k++) {
			this.matReqRepository.deleteById(mrList.get(k).getId());
		}
		
		this.matReqRepository.flush();
		boolean flag = this.prodScheduleAService.saveMatRequCalc("prod_week_term", pwtId,"material", user.getId());
		
		result.success = flag;
		
		return result;
		
	}
	
	// 원부자재 재고 예약
	@PostMapping("/rawmat_reservation")
	@Transactional
	public AjaxResult rawmatReservation(
			@RequestBody MultiValueMap<String,Object> Q,
			HttpServletRequest request,
			Authentication auth) {
		
		User user = (User)auth.getPrincipal();
		
		AjaxResult result = new AjaxResult();
		
		List<Map<String, Object>> data = CommonUtil.loadJsonListMap(Q.getFirst("Q").toString());
	
		for (int i=0; i<data.size(); i++) {
			Integer id = Integer.parseInt(data.get(i).get("id").toString());
			Integer reservationStockInput = Integer.parseInt(data.get(i).get("ReservationStock_input").toString());
			
			MatRequ mr = this.matReqRepository.getMatReqById(id);
			mr.setReservationStock((float)reservationStockInput);
			mr.set_audit(user);
			mr = this.matReqRepository.save(mr);
			result.data = mr;
		}
		
		return result;
	}
	
	// 원부자재 구매요청량 저장
	@PostMapping("/rawmat_qty_purchase_save")
	@Transactional
	public AjaxResult rawmatQtyPurchaseSave(
			@RequestParam("pwt_id") String pwtId,
			@RequestBody MultiValueMap<String,Object> Q,
			HttpServletRequest request,
			Authentication auth) {
		
		User user = (User)auth.getPrincipal();
		
		AjaxResult result = new AjaxResult();
		
		Timestamp today = new Timestamp(System.currentTimeMillis());
		
		List<Map<String, Object>> data = CommonUtil.loadJsonListMap(Q.getFirst("Q").toString());
	
		for (int i=0; i<data.size(); i++) {
			Integer id = Integer.parseInt(data.get(i).get("id").toString());
			Integer requestQty = Integer.parseInt(data.get(i).get("RequestQty").toString());
			
			MatRequ mr = this.matReqRepository.getMatReqById(id);
			mr.setRequestQty((float)requestQty);
			mr.setRequestDate(today);
			mr.set_audit(user);
			mr = this.matReqRepository.save(mr);
			result.data = mr;
		}
		
		return result;
	}
	
	// 원부자재 구매요청량 확정
	@PostMapping("/rawmat_qty_purchase_confirm")
	@Transactional
	public AjaxResult rawmatQtyPurchaseConfirm(
			@RequestParam("pwt_id") String pwtId,
			@RequestBody MultiValueMap<String,Object> Q,
			HttpServletRequest request,
			Authentication auth) {
		
		User user = (User)auth.getPrincipal();
		
		AjaxResult result = new AjaxResult();
	
		ProdWeekTerm pt = this.prodWeekTermRepository.getProdWeekTermById(Integer.parseInt(pwtId));
		
		pt.setState("material");
		pt.set_audit(user);
		
		String StateName = "";
		SystemCode queryset = this.sysCodeRepository.findByCodeTypeAndCode("prod_week_term_state","material");
		if (queryset != null) {
			StateName = queryset.getValue();
		}
		
		result.StateName = StateName;
		result.data = pt;
		
		return result;
	}
}
