package mes.app.sales;

import java.util.List;
import java.util.Map;
import java.sql.Timestamp;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.sales.service.SujuManageService;
import mes.domain.entity.Suju;
import mes.domain.model.AjaxResult;
import mes.domain.repository.SujuRepository;
import mes.domain.services.CommonUtil;

@RestController
@RequestMapping("/api/sales/suju_manage")
public class SujuManageController {

	@Autowired 
	SujuRepository SujuRepository;
	
	@Autowired
	private SujuManageService SujuManageService;
	
	// 수주 목록 조회
	@GetMapping("/read")
	public AjaxResult getSujuList(
			@RequestParam(value="data_startdate", required=false) String start_date,
			@RequestParam(value="data_enddate", required=false) String end_date,
			@RequestParam(value="date_kind", required=false) String date_kind,
			@RequestParam(value="all_yn", required=false) boolean all_yn,
			HttpServletRequest request) {
		
		start_date = start_date + " 00:00:00";
		end_date = end_date + " 23:59:59";
		
		Timestamp start = Timestamp.valueOf(start_date);
		Timestamp end = Timestamp.valueOf(end_date);
		
		List<Map<String, Object>> items = this.SujuManageService.getSujuList(start, end, date_kind, all_yn);
		
		AjaxResult result = new AjaxResult();
        result.data = items;        				
        
		return result;
	}
	
	//수주 취소
	@GetMapping("/cancel")
	public AjaxResult CancelSuju(
			@RequestParam("suju_id") int id,
			HttpServletRequest request) {
		
		Suju suju = null;
		suju = this.SujuRepository.getSujuById(id);
		suju.setState("canceled");
		suju = this.SujuRepository.save(suju);
		
		AjaxResult result = new AjaxResult();
		result.data = suju;
		return result;
	}
	
	// 수주 복구
	@GetMapping("/restore")
	public AjaxResult RestoreSuju(
			@RequestParam("suju_id") int id,
			HttpServletRequest request) {
		
		Suju suju = null;
		suju = this.SujuRepository.getSujuById(id);
		suju.setState("received");
		suju = this.SujuRepository.save(suju);
		
		AjaxResult result = new AjaxResult();
		result.data = suju;
		return result;
	}
	
	// 생산계획일 지정
	@PostMapping("/plan_appoint")
	public AjaxResult SujuPlanDate(
			@RequestParam("Q") String suju_list,
			HttpServletRequest request) {
		AjaxResult result = new AjaxResult();
		//User user = (User)auth.getPrincipal();
		List<Map<String, Object>> items = CommonUtil.loadJsonListMap(suju_list);
		for (int i=0; i<items.size(); i++) {
			Map<String, Object> item = items.get(i);
			System.out.println(item);
			int suju_id = (Integer)item.get("suju_id");
			String PlanDate = (String)item.get("plan_date");
			PlanDate = PlanDate + " 00:00:00";
			Timestamp Date = Timestamp.valueOf(PlanDate);
			Suju suju = this.SujuRepository.getSujuById(suju_id);
			suju.setProductionPlanDate(Date);
			//sujumanage.set_audit(user);
			suju = this.SujuRepository.save(suju);
			
			result.data = suju;		
		}
	
		return result;
	}
}
