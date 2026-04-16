package mes.app.precedence;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.precedence.service.PartnerCheckService;
import mes.domain.entity.BundleHead;
import mes.domain.entity.TestItemResult;
import mes.domain.entity.TestMaster;
import mes.domain.entity.TestResult;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.BundleHeadRepository;
import mes.domain.repository.TestItemResultRepository;
import mes.domain.repository.TestMasterRepository;
import mes.domain.repository.TestResultRepository;
import mes.domain.services.CommonUtil;
import mes.domain.services.SqlRunner;

@RestController
@RequestMapping("/api/precedence/partner_check")
public class PartnerCheckController {
	@Autowired
	PartnerCheckService partnerCheckService;
	
	@Autowired
	BundleHeadRepository bundleHeadRepository;
	
	@Autowired
	TestItemResultRepository testItemResultRepository;
	
	@Autowired
	SqlRunner sqlRunner;
	
	@Autowired
	TestMasterRepository testMasterRepository;
	
	@Autowired
	TestResultRepository testResultRepository;
	
	
	@GetMapping("/read")
	private AjaxResult Result(
			@RequestParam("start_date") String start_date,
			@RequestParam("end_date") String end_date) {
		
		List<Map<String, Object>> items = this.partnerCheckService.getList(start_date, end_date);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	
	
	@GetMapping("/ListRead")
	private AjaxResult apprList(
			@RequestParam(value="bhId", required=false) Integer bhId,
			@RequestParam("data_date") String data_date,
			Authentication auth,
			HttpServletRequest request) {
		
		Map<String, Object> items = this.partnerCheckService.apprList(bhId,data_date,auth);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	
	
	@PostMapping("/save")
	@Transactional(isolation = Isolation.READ_UNCOMMITTED)
	private AjaxResult save(
			@RequestParam(value="bhId", required=false) Integer bhId,
			@RequestParam(value="title", required=false) String title,
			@RequestParam(value="necessity", required=false) String necessity,
			@RequestParam(value="cboCompany", required=false) String cboCompany,
			@RequestParam("data_date") String data_date,
			@RequestParam (value="headInfo") String headInfo,
			@RequestParam (value="diaryInfo") String diaryInfo,
			Authentication auth,
			HttpServletRequest request) throws JSONException {
		
		User user = (User)auth.getPrincipal();
		
		JSONObject headInfo1 = new JSONObject(headInfo);
		
		List<Map<String, Object>> diaryInfo1 = CommonUtil.loadJsonListMap(diaryInfo);
		
		AjaxResult result = new AjaxResult();
		
		Timestamp checkDate = Timestamp.valueOf(data_date+ " 00:00:00");
		
		BundleHead bh = new BundleHead();
		
		if(bhId > 0) {
			bh = this.bundleHeadRepository.getBundleHeadById(bhId);		

		}else {
			bh.setDate1(checkDate);
			bh.setTableName("partner_check");
		}
		bh.setChar1(headInfo1.get("Title").toString());
		bh.setChar2(necessity);
		bh.set_audit(user);
		this.bundleHeadRepository.save(bh);
		
		bhId = bh.getId();
		
		List<TestMaster> row =  testMasterRepository.findByName("협력업체점검표");
		
		Integer tmId =  row.get(0).getId();
		
		TestResult tr = new TestResult();
		
		List<TestResult> trList = this.testResultRepository.findBySourceTableNameAndSourceDataPk("partner_check",bhId);
		
		if(trList.size() > 0) {
			tr = trList.get(0);
		} else {
			tr.setSourceDataPk(bhId);
			tr.setSourceTableName("partner_check");
			tr.setTestDateTime(checkDate);
		}
		
		tr.setTestMasterId(tmId);
		tr.setMaterialId(0);
		tr.set_audit(user);
		this.testResultRepository.save(tr);
		
		Integer trId = tr.getId();
		
		List<TestItemResult> tirList = this.testItemResultRepository.findByTestResultId(trId);
		
		for (TestItemResult list: tirList) {
			this.testItemResultRepository.deleteById(list.getId());
		}
		
		for(int i=0; i<diaryInfo1.size(); i++) {
			TestItemResult tir = new TestItemResult();
			tir.setTestItemId((Integer) diaryInfo1.get(i).get("TestItem_id"));
			tir.setChar1(diaryInfo1.get(i).get("Char1").toString());
			tir.setChar2(diaryInfo1.get(i).get("Char2").toString());
			if(diaryInfo1.get(i).get("CharResult") != null) {
				tir.setCharResult(diaryInfo1.get(i).get("CharResult").toString());
			}else {
				tir.setCharResult("");
			}
			tir.setInputResult(cboCompany);
			tir.setTestDateTime(checkDate);
			tir.setTestResultId(trId);
			tir.set_audit(user);
			
			this.testItemResultRepository.save(tir);
		}
		
		
		 Map<String, Object> items = new HashMap<>();
	        items.put("id", bhId);
	        
	        result.data = items;
	        
			return result;
	}
	
	@PostMapping("/delete")
	public AjaxResult mstDelete(
			@RequestParam(value="bhId", required=false) Integer bhId) {
		
		AjaxResult result = new AjaxResult();
		
        this.partnerCheckService.mstDelete(bhId);
        
        result.success = true;
        
        return result;
	}
}
