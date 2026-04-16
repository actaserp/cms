package mes.app.production;

import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.production.service.ProdResultListService;
import mes.domain.entity.JobRes;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.JobResRepository;

@RestController
@RequestMapping("/api/production/prod_result_list")
public class ProdResultListController {

	@Autowired
	private ProdResultListService prodResultListService;

	@Autowired
	private JobResRepository jobResRepository;
	
	// 작업목록
	@GetMapping("/read")
	public AjaxResult getProdResultList(
			@RequestParam(value="date_from", required=false) String date_from,
			@RequestParam(value="date_to", required=false) String date_to,
			@RequestParam(value="shift_code", required=false) String shift_code,
			@RequestParam(value="workcenter_pk", required=false) Integer workcenter_pk,
			@RequestParam("spjangcd") String spjangcd,
			HttpServletRequest request) {

		List<Map<String, Object>> items = this.prodResultListService.getProdResultList(date_from, date_to, shift_code, workcenter_pk, spjangcd);
        AjaxResult result = new AjaxResult();
        result.data = items;
		return result;
	}
		
	// 상세내역
	@GetMapping("/detail")
	public AjaxResult getProdResultDetail(
			@RequestParam("mp_pk") int mp_pk,
			HttpServletRequest request) {
		Map<String, Object> item = this.prodResultListService.getProdResultDetail(mp_pk);
		
		AjaxResult result = new AjaxResult();
		result.data = item;
		
		return result;
	}

	// 부적합내역 조회
	@GetMapping("/defect_list")
	public AjaxResult getProdResultDefectList(
			@RequestParam("jr_pk") int jr_pk,
			HttpServletRequest request) {
		List<Map<String, Object>> items = this.prodResultListService.getProdResultDefectList(jr_pk);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}

	// 투입목록조회
	@GetMapping("/consumed_list")
	public AjaxResult getProdResultConsumedList(
			@RequestParam("mp_pk") int mp_pk,
			HttpServletRequest request) {
		List<Map<String, Object>> items = this.prodResultListService.getProdResultConsumedList(mp_pk);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}

	// 비고 저장
	@PostMapping("/save_description")
	public AjaxResult saveDesctiprion(
			@RequestParam(value="jr_pk", required=false) Integer jr_pk,
			@RequestParam(value="description") String description,
			HttpServletRequest request,
			Authentication auth	) {
		
		User user = (User)auth.getPrincipal();
		
		JobRes jopRes =  this.jobResRepository.getJobResById(jr_pk);
		jopRes.setDescription(description);
		jopRes.set_audit(user);
		
		this.jobResRepository.save(jopRes);
		
		AjaxResult result = new AjaxResult();
		result.data = jopRes;
		return result;
	}

	// 작업목록
	@GetMapping("/read_process")
	public AjaxResult getProdProcessList(
			@RequestParam(value="date_from", required=false) String date_from,
			@RequestParam(value="date_to", required=false) String date_to,
			@RequestParam(value="process_pk", required=false) Integer process_pk,
			@RequestParam("spjangcd") String spjangcd,
			HttpServletRequest request) {

		List<Map<String, Object>> items = this.prodResultListService.getProdProcessList(date_from, date_to, process_pk, spjangcd);
		AjaxResult result = new AjaxResult();
		result.data = items;
		return result;
	}
	
}
