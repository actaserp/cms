package mes.app.test;

import java.util.List;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.transaction.Transactional;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.security.core.Authentication;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.test.service.TestMasterService;
import mes.domain.entity.TestMaster;
import mes.domain.entity.TestMasterItem;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.TestMasterItemRepository;
import mes.domain.repository.TestMasterRepository;
import mes.domain.services.CommonUtil;
import mes.domain.services.SqlRunner;

@RestController
@RequestMapping("/api/test/test_master")
public class TestMasterController{
	
	@Autowired
	TestMasterRepository testMasterRepository;
	
	@Autowired
	TestMasterItemRepository testMasterItemRepository;
	
	@Autowired
	private TestMasterService testMasterService;

	@Autowired
	SqlRunner sqlRunner;
	
	@GetMapping("/read")
	public AjaxResult getTestMasterList(
			@RequestParam (value="name", required=false) String name ,
			@RequestParam (value= "test_class", required=false) String testClass,
			HttpServletRequest request) {
		List<Map<String, Object>> items = this.testMasterService.getTestMasterList(name,testClass);
		AjaxResult result = new AjaxResult();
		result.data = items;
		return result;
		}

	
	@GetMapping("/detail")
	public AjaxResult getTestMaster(@RequestParam("id") int id) {
		Map<String, Object> item = this.testMasterService.getTestMasterDetail(id);
		AjaxResult result= new AjaxResult();
		result.data = item;
		return result;
	}
	
	@PostMapping("/save")
	@Transactional
	public AjaxResult saveTestMaster(
			@RequestParam(value="id", required=false) Integer id,
			@RequestParam(value="master_grp", required= false) Integer master_grp,
			@RequestParam(value="master_name") String master_name,
			@RequestParam(value="test_type") String test_type,
			HttpServletRequest request,
			Authentication auth) {

		AjaxResult result=new AjaxResult();
		
		User user = (User)auth.getPrincipal();
		
		TestMaster tm = null;
		if(id == null) {
			tm = new TestMaster();
		} else {
			tm = this.testMasterRepository.getTestMasterById(id);
		}

		boolean valid_check = this.testMasterRepository.findByName(master_name).isEmpty();
		
		if (master_name.equals(tm.getName()) == false && valid_check == false) {
			result.success = false;
			result.message = "중복된 마스터명이 있습니다. 마스터명을 변경해주세요.";	// "중복된 마스터명이 있습니다. \\n 마스터명을 변경해주세요.";
			return result;
		}
		
		tm.setName(master_name);
		tm.setTesttype(test_type);
		tm.setTestmastergroup_id(master_grp);
		tm.set_audit(user);
		
		tm = this.testMasterRepository.save(tm);
		
		result.data = tm;
		return result;
				
	}
	
	@PostMapping("/delete")
	@Transactional
	public AjaxResult deleteTestMaster(@RequestParam("id") int id) {

		List<TestMasterItem> test_item_master = this.testMasterItemRepository.getByTestMasterId(id);
		
		for (int i = 0; i < test_item_master.size(); i++) {
			
			if (test_item_master.get(i) != null && test_item_master.get(i).getId() != null) {
				
				Integer testItemMasterid = test_item_master.get(i).getId();
				this.testMasterItemRepository.deleteById(testItemMasterid);
			}
		}
		
		this.testMasterRepository.deleteById(id);
		AjaxResult result = new AjaxResult();
		return result;
	}
	
	@GetMapping("/get_item_info")
	public AjaxResult getItemInfo(@RequestParam("item_id") int item_id) {
		Map<String,Object> item = this.testMasterService.getItemInfo(item_id);
		AjaxResult result = new AjaxResult();
		result.data = item;
		return result;
		
	}
	
	
	@GetMapping("/item_list")
	public AjaxResult getItemList(@RequestParam ("master_id") int master_id) {
		List<Map<String, Object>> items = this.testMasterService.getItemList(master_id);
		AjaxResult result = new AjaxResult();
		result.data = items;
		return result;
		
	}
	@GetMapping("/get_applied_mat")
	public AjaxResult getAppliedMat(
			@RequestParam(value="master_id") int id,
			HttpServletRequest request) {
		List<Map<String, Object>> items = this.testMasterService.getAppliedMat(id);
		AjaxResult result = new AjaxResult();
		result.data = items;
		return result;
		
	}
	
	@PostMapping("/save_item")
	@Transactional
	public AjaxResult saveItem(
			@RequestParam("value") String value,
			HttpServletRequest request,
			Authentication auth) throws JSONException {

		AjaxResult result = new AjaxResult();
		
		User user = (User)auth.getPrincipal();
				
		JSONObject json = new JSONObject(value);
		String masterId = (String)json.get("master_id");
		JSONArray item_list = json.getJSONArray("item_list");
		JSONArray del_ids = json.getJSONArray("del_ids");
		
		for (int j=0; j<del_ids.length(); j++) {
			this.testMasterItemRepository.deleteById((Integer) del_ids.get(j));
		}

		// 순서변경을 위해 미리 저장되어 있던 검사항목 UPDATE
		Integer master_id = Integer.parseInt(masterId);
		
		String sql = """
            update test_item_mast 
            set _order = _order * -100
            where "TestMaster_id" = :master_id				
				""";

        MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("master_id", master_id);
        
        this.sqlRunner.execute(sql, dicParam);
        
		for (int i=0; i<item_list.length(); i++) {
			// 저장
			TestMasterItem tmi = null;
			Integer item_master_id = StringUtils.hasText(item_list.getJSONObject(i).get("id").toString()) ? (Integer) item_list.getJSONObject(i).get("id") : null;
            Integer item_id = StringUtils.hasText(item_list.getJSONObject(i).get("test_item_id").toString()) ? (Integer) item_list.getJSONObject(i).get("test_item_id") : null;
            Integer round_digit = StringUtils.hasText(item_list.getJSONObject(i).get("round_digit").toString()) ? CommonUtil.tryInt(item_list.getJSONObject(i).get("round_digit")) : null;
            String spec_type = StringUtils.hasText(item_list.getJSONObject(i).get("spec_type").toString()) ? (String) item_list.getJSONObject(i).get("spec_type") : null;
            Float low_spec = StringUtils.hasText(item_list.getJSONObject(i).get("low_spec").toString()) ? Float.parseFloat(String.valueOf(item_list.getJSONObject(i).get("low_spec"))) : null;
            Float upper_spec = StringUtils.hasText(item_list.getJSONObject(i).get("upper_spec").toString()) ? Float.parseFloat(String.valueOf(item_list.getJSONObject(i).get("upper_spec"))) : null;
            String spec_text = StringUtils.hasText(item_list.getJSONObject(i).get("spec_text").toString()) ? (String) item_list.getJSONObject(i).get("spec_text") : null;
            String eng_spec_text = StringUtils.hasText(item_list.getJSONObject(i).get("eng_spec_text").toString()) ? (String) item_list.getJSONObject(i).get("eng_spec_text") : null;
            
			if (item_master_id == null) {
				tmi = new TestMasterItem();
				tmi.setTestMasterId(master_id);
				tmi.setTestItem_id(item_id);				
			} else {
				tmi = this.testMasterItemRepository.getTestMasterItemById(item_master_id);
			}
			tmi.setRoundDigit(round_digit);
			tmi.setSpecType(spec_type);
			tmi.setLowSpec(low_spec);
			tmi.setUpperSpec(upper_spec);
			tmi.setSpecText(spec_text);
			tmi.setEngSpecText(eng_spec_text);
			tmi.set_order(i + 1);
			tmi.set_audit(user);
			
			tmi = this.testMasterItemRepository.save(tmi);
			
			result.data = tmi;
		}
		
		return result;
	}
	
}