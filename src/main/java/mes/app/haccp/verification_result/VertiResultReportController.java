package mes.app.haccp.verification_result;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.transaction.Transactional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.security.core.Authentication;
import org.springframework.util.MultiValueMap;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.common.service.FileService;
import mes.app.haccp.verification_result.service.VertiResultReportService;
import mes.domain.entity.BundleHead;
import mes.domain.entity.CheckItemResult;
import mes.domain.entity.CheckMaster;
import mes.domain.entity.CheckResult;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.BundleHeadRepository;
import mes.domain.repository.CheckItemResultRepository;
import mes.domain.repository.CheckMasterRepository;
import mes.domain.repository.CheckResultRepository;
import mes.domain.services.CommonUtil;

@RestController
@RequestMapping("/api/haccp/verification_result_report")
public class VertiResultReportController {
	
	@Autowired
	private VertiResultReportService vertiResultReportService;
	
	@Autowired
	BundleHeadRepository bundleHeadRepository;
	
	@Autowired
	CheckResultRepository checkResultRepository;
	
	@Autowired
	CheckItemResultRepository checkItemResultRepository;
	
	@Autowired
	CheckMasterRepository checkMasterRepository;
	
	@Autowired
	FileService fileService;
	
	@GetMapping("/read")
	public AjaxResult getVertiResultReport(
			@RequestParam(value="check_master_id", required=false) Integer checkMasterId, 
			@RequestParam(value="start_date", required=false) String startDate, 
			@RequestParam(value="end_date", required=false) String endDate, 
			@RequestParam(value="appr_status", required=false) String apprStatus, 
			HttpServletRequest request) {
		
		List<CheckMaster> cm = this.checkMasterRepository.findByName("검증 결과보고서");
		if (cm.size() > 0) {
			checkMasterId = cm.get(0).getId();
		}
		
		List<Map<String,Object>> items = this.vertiResultReportService.getVertiResultReport(checkMasterId,startDate,endDate,apprStatus);
		AjaxResult result = new AjaxResult();
		result.data = items;
		return result;
	}
	
	@GetMapping("/result_list")
	public AjaxResult getResultList(
			@RequestParam(value="bh_id", required=false) Integer bhId,
			HttpServletRequest request) {
		
		Map<String, Object> items = this.vertiResultReportService.getResultList(bhId);
		AjaxResult result = new AjaxResult();
		result.data = items;
		return result;
	}

	@SuppressWarnings("unchecked")
	@PostMapping("/insert")
	@Transactional
	public AjaxResult insertVertiResultReport(
			@RequestParam(value="check_master_id", required=false, defaultValue= "0") Integer checkMasterId,
			@RequestParam(value="title", required=false) String title,
			@RequestParam(value="check_date", required=false) String check_date,
			@RequestParam MultiValueMap<String,Object> Q,
			@RequestParam(value="fileId", required=false) String fileId,
			HttpServletRequest request,
			Authentication auth) {
		
		User user = (User)auth.getPrincipal();
		
		AjaxResult result = new AjaxResult();
		
		Timestamp checkDate = Timestamp.valueOf(check_date+ " 00:00:00");
		
		BundleHead bh = new BundleHead();
		bh.setTableName("verti_result_report");
		bh.setChar1(title);
		bh.setDate1(checkDate);
		bh.setNumber1((float)checkMasterId);
		bh.set_audit(user);
		
		this.bundleHeadRepository.save(bh);
		
		Integer bhId = bh.getId();
		
		List<Map<String, Object>> items = CommonUtil.loadJsonListMap(Q.getFirst("Q").toString());
		
		for(int i = 0; i < items.size(); i++) {
			CheckResult cr = new CheckResult();
			cr.setSourceDataPk(bhId);
			cr.setCheckMasterId(checkMasterId);
			cr.setCheckDate(Date.valueOf(check_date));
			cr.setCheckerName(user.getUserProfile().getName());
			
			if (items.get(i).containsKey("description")) {
				if (items.get(i).get("description") != null) {
					cr.setDescription(items.get(i).get("description").toString());
				}
			}
			
			if (items.get(i).containsKey("char1")) {
				if (items.get(i).get("char1") != null) {
					cr.setChar1(items.get(i).get("char1").toString());
				}
			}
			
			cr.set_audit(user);
			this.checkResultRepository.save(cr);
			Integer checkResultId = cr.getId();
			
			List<Map<String, Object>> items2 = (List<Map<String, Object>>) items.get(i).get("tabList");
			
			for (int j = 0; j < items2.size(); j++) {
				CheckItemResult cir = new CheckItemResult();
				cir.setCheckResultId(checkResultId);
				cir.setCheckItemId(Integer.parseInt(items2.get(j).get("id").toString()));
				
				if (items2.get(j).get("result1") != null) {
					cir.setResult1(items2.get(j).get("result1").toString());
				} else {
					cir.setResult1(null);
				}
				
				if (items2.get(j).get("index_order") != null) {
					cir.setOrder(Integer.parseInt(items2.get(j).get("index_order").toString()));
				}
                
				cir.set_audit(user);
				this.checkItemResultRepository.save(cir);
			}
		}
		
		if (StringUtils.hasText(fileId)) {
			Integer dataPk = bh.getId();
			String[] fileIdList = fileId.split(",");
			
			for (int i = 0; i < fileIdList.length; i++) {
				fileService.updateDataPk(Integer.parseInt(fileIdList[i]), dataPk);
			}
		}
		
		Map<String, Object> value = new HashMap<String, Object>();
		value.put("id", bh.getId());
		
		result.data = value;
		
		return result;
	}
	
	@SuppressWarnings("unchecked")
	@PostMapping("/save")
	@Transactional
	public AjaxResult savetVertiResultReport(
			@RequestParam(value="bh_id", required=false, defaultValue= "0") Integer bhId,
			@RequestParam(value="title", required=false) String title,
			@RequestParam(value="check_date", required=false) String check_date,
			@RequestParam MultiValueMap<String,Object> Q,
			@RequestParam(value="fileId", required=false) String fileId,
			HttpServletRequest request,
			Authentication auth) {
		
		User user = (User)auth.getPrincipal();
		
		AjaxResult result = new AjaxResult();
		
		Timestamp checkDate = Timestamp.valueOf(check_date+ " 00:00:00");
		
		BundleHead bh = this.bundleHeadRepository.getBundleHeadById(bhId);
		bh.setChar1(title);
		bh.setDate1(checkDate);
		bh.set_audit(user);
		
		this.bundleHeadRepository.save(bh);
		
		List<Map<String, Object>> items = CommonUtil.loadJsonListMap(Q.getFirst("Q").toString());
		
		for(int i = 0; i < items.size(); i++) {
			
			CheckResult cr = this.checkResultRepository.getCheckResultById(Integer.parseInt(items.get(i).get("id").toString()));
			cr.setCheckerName(user.getUserProfile().getName());
			
			if (items.get(i).containsKey("description")) {
				if (items.get(i).get("description") != null) {
					cr.setDescription(items.get(i).get("description").toString());
				}
			}
			
			
			cr.set_audit(user);
			this.checkResultRepository.save(cr);
			
			List<Map<String, Object>> items2 = (List<Map<String, Object>>) items.get(i).get("item_result");
			
			for (int j = 0; j < items2.size(); j++) {
				CheckItemResult cir = this.checkItemResultRepository.getCheckItemResultById(Integer.parseInt(items2.get(j).get("id").toString()));
				
				if (items2.get(j).get("result1") != null) {
					cir.setResult1(items2.get(j).get("result1").toString());
				} else {
					cir.setResult1(null);
				}
                
				cir.set_audit(user);
				this.checkItemResultRepository.save(cir);
			}
		}
		
		if (StringUtils.hasText(fileId)) {
			Integer dataPk = bh.getId();
			String[] fileIdList = fileId.split(",");
			
			for (int i = 0; i < fileIdList.length; i++) {
				fileService.updateDataPk(Integer.parseInt(fileIdList[i]), dataPk);
			}
		}
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("bhId", bhId);
		paramMap.addValue("userId", user.getId());
		
		
		Map<String, Object> value = new HashMap<String, Object>();
		value.put("id", bhId);
		
		result.data = value;
		
		return result;
	}
	
	@PostMapping("/report_delete")
	public AjaxResult mstDelete(
			@RequestParam(value="bh_id", required=false, defaultValue= "0") Integer bhId,
			HttpServletRequest request,
			Authentication auth) {
		
		AjaxResult result = new AjaxResult();
		
		this.vertiResultReportService.reportDelete(bhId);
		
		result.success = true;
		
		return result;
	}
}
