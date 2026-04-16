package mes.app.precedence;

import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.transaction.Transactional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.precedence.service.IllumResultStatService;
import mes.domain.model.AjaxResult;
import mes.domain.repository.BundleHeadRepository;
import mes.domain.repository.CheckItemResultRepository;
import mes.domain.repository.CheckMasterRepository;
import mes.domain.repository.CheckResultRepository;
import mes.domain.repository.DeviationActionRepository;
import mes.domain.repository.MasterResultRepository;
import mes.domain.services.SqlRunner;

@RestController
@RequestMapping("/api/precedence/illum_result_stat")
public class IllumResultStatController {
	
	@Autowired
	private IllumResultStatService illumResultStatService;
	
	@Autowired
	CheckMasterRepository checkMasterRepository; 
	
	@Autowired
	CheckResultRepository checkResultRepository;
	
	@Autowired
	CheckItemResultRepository checkItemResultRepository;

	@Autowired
	BundleHeadRepository bundleHeadRepository;
	
	@Autowired
	DeviationActionRepository deviationActionRepository;
	
	@Autowired
	MasterResultRepository masterResultRepository;
	
	@Autowired
	SqlRunner sqlRunner;

	// 결재현황 조회
	@GetMapping("/appr_stat")
	public AjaxResult getIllumResultApprStat(
    		@RequestParam("data_year") String data_year, 
    		@RequestParam(value="data_month", required=false) String data_month,
    		@RequestParam(value="appr_state", required=false) String appr_state,
			HttpServletRequest request) {
		
        List<Map<String, Object>> items = this.illumResultStatService.getIllumResultApprStat(data_year, data_month, appr_state);      
        AjaxResult result = new AjaxResult();
        result.data = items;        
		return result;
	}
	
	// 조도데이터 전체삭제
	@PostMapping("/delete")
	@Transactional
	public AjaxResult deleteIllumResultApprStat(
			@RequestParam("bh_id") Integer bh_id) {
		
		AjaxResult result = new AjaxResult();
		
		this.bundleHeadRepository.deleteById(bh_id);
    	this.masterResultRepository.deleteByMasterClassAndNumber2("illum_zone", bh_id);
		
		result.success = true;
		return result;
	}
}
