package mes.app.haccp;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.transaction.Transactional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.haccp.service.ProdTestService;
import mes.domain.entity.BundleHead;
import mes.domain.entity.RelationData;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.BundleHeadRepository;
import mes.domain.repository.RelationDataRepository;
import mes.domain.services.CommonUtil;

@RestController
@RequestMapping("/api/haccp/prod_test")
public class ProdTestController {

	@Autowired
	private ProdTestService prodTestService;
	
	@Autowired
	BundleHeadRepository bundleHeadRepository;
	
	@Autowired
	RelationDataRepository relationDataRepository;
	
	@GetMapping("/read_list")
	public AjaxResult getReadList(
			@RequestParam(value="start_date", required=false) String startDate, 
			@RequestParam(value="end_date", required=false) String endDate,
			HttpServletRequest request
			) {
		AjaxResult result = new AjaxResult();
		
		List<Map<String,Object>> items = this.prodTestService.getReadList(startDate,endDate);
		
		result.data = items;
		
		return result;
	}
	
	@GetMapping("/result_list")
	public AjaxResult getResultList(
			@RequestParam(value="bh_id", required=false) Integer bhId,
			@RequestParam(value="srchStartDt", required=false) String srchStartDt, 
			@RequestParam(value="srchEndDt", required=false) String srchEndDt,
			@RequestParam(value="first", required=false) Boolean first,
			HttpServletRequest request) {
		
		AjaxResult result = new AjaxResult();
		
		Map<String, Object> items = null;
		
		if(bhId > 0 && first) {
			items = this.prodTestService.getResultList(bhId);
		} else {
			items = this.prodTestService.getResultListDefault(srchStartDt,srchEndDt);      
		}
		
		
        result.data = items;
        
		return result;
	}
	
	@SuppressWarnings("unchecked")
	@PostMapping("/insert")
	@Transactional
	public AjaxResult insertMatInoutStock(
			@RequestParam(value="title", required=false) String title,
			@RequestParam(value="check_date", required=false) String check_date,
			@RequestParam(value="srchStartDt", required=false) String srchStartDt,
			@RequestParam(value="srchEndDt", required=false) String srchEndDt,
			@RequestParam MultiValueMap<String,Object> Q,
			HttpServletRequest request,
			Authentication auth) {
		
		User user = (User)auth.getPrincipal();
		
		AjaxResult result = new AjaxResult();
		
		Timestamp checkDate = Timestamp.valueOf(check_date+ " 00:00:00");
		
		BundleHead bh = new BundleHead();
		bh.setTableName("prod_test_result");
		bh.setChar1(title);
		bh.setChar2(srchStartDt);
		bh.setChar3(srchEndDt);
		bh.setDate1(checkDate);
		bh.set_audit(user);
		
		this.bundleHeadRepository.save(bh);
		
		Integer bhId = bh.getId();
		
		List<Map<String, Object>> items = CommonUtil.loadJsonListMap(Q.getFirst("Q").toString());
		
		List<Map<String, Object>> itemList = (List<Map<String, Object>>) items.get(0).get("tabList");
		
		for(int i = 0; i < itemList.size(); i++) {
			RelationData rd = new RelationData();
			
			rd.setDataPk1(bhId);
			rd.setTableName1("bundle_head");
			rd.setDataPk2(Integer.parseInt(itemList.get(i).get("id").toString()));
			rd.setTableName2("job_res");
			rd.setRelationName("prod_test_result");
			rd.set_audit(user);
			this.relationDataRepository.saveAndFlush(rd);
		}
		
		Map<String, Object> value = new HashMap<String, Object>();
		
		value.put("id", bh.getId());
		value.put("srchStartDt", srchStartDt);
		value.put("srchEndDt", srchEndDt);
		
		result.data = value;
		
		return result;
	}
	
	@PostMapping("/save")
	@Transactional
	public AjaxResult saveMatInoutStock(
			@RequestParam(value="bh_id", required=false, defaultValue= "0") Integer bhId,
			@RequestParam(value="title", required=false) String title,
			@RequestParam(value="check_date", required=false) String check_date,
			@RequestParam(value="srchStartDt", required=false) String srchStartDt,
			@RequestParam(value="srchEndDt", required=false) String srchEndDt,
			@RequestParam MultiValueMap<String,Object> Q,
			HttpServletRequest request,
			Authentication auth) {
		
		User user = (User)auth.getPrincipal();
		
		AjaxResult result = new AjaxResult();
		
		Timestamp checkDate = Timestamp.valueOf(check_date+ " 00:00:00");
		
		BundleHead bh =this.bundleHeadRepository.getBundleHeadById(bhId);
		bh.setChar1(title);
		bh.setChar2(srchStartDt);
		bh.setChar3(srchEndDt);
		bh.setDate1(checkDate);
		bh.set_audit(user);
		
		this.bundleHeadRepository.save(bh);
		
		List<Map<String, Object>> itemList = CommonUtil.loadJsonListMap(Q.getFirst("Q").toString());
		
		List<RelationData> rdList = this.relationDataRepository.findByDataPk1AndTableName1AndRelationNameAndTableName2(bhId, "bundle_head", "prod_test_result", "job_res");
		
		for(int i = 0; i < rdList.size(); i++) {
			this.relationDataRepository.deleteById(rdList.get(i).getId());
		}
		
		this.relationDataRepository.flush();
		
		for(int i = 0; i < itemList.size(); i++) {
			RelationData rd = new RelationData();
			
			rd.setDataPk1(bhId);
			rd.setTableName1("bundle_head");
			rd.setDataPk2(Integer.parseInt(itemList.get(i).get("id").toString()));
			rd.setTableName2("job_res");
			rd.setRelationName("prod_test_result");
			rd.set_audit(user);
			this.relationDataRepository.saveAndFlush(rd);
		}
		
		Map<String, Object> value = new HashMap<String, Object>();
		value.put("id", bhId);
		
		result.data = value;
		
		return result;
	}
	
	@PostMapping("/mst_delete")
	public AjaxResult mstDelete(
			@RequestParam(value="bh_id", required=false, defaultValue= "0") Integer bhId,
			HttpServletRequest request,
			Authentication auth) {
		
		AjaxResult result = new AjaxResult();
		
		this.prodTestService.mstDelete(bhId);
		
		result.success = true;
		
		return result;
	}
}
