package mes.app.precedence;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.transaction.Transactional;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.definition.service.UserCodeService;
import mes.domain.entity.RelationData;
import mes.domain.entity.User;
import mes.domain.entity.UserCode;
import mes.domain.model.AjaxResult;
import mes.domain.repository.RelationDataRepository;
import mes.domain.repository.UserCodeRepository;
import mes.domain.services.CommonUtil;

@RestController
@RequestMapping("/api/precedence/pest_trap")
public class PestTrapController {

	
	@Autowired
	private UserCodeService userCodeService;
	
	@Autowired
	UserCodeRepository userCodeRepository;
	
	@Autowired
	RelationDataRepository relationDataRepository;
	
	@GetMapping("/read")
	public AjaxResult getUserCode(
			@RequestParam(value="parent_code") String parentCode,
			@RequestParam(value="type", required=false) String type,
			@RequestParam(value="type_class_code", required=false) String typeClassCode,
			@RequestParam(value="type2_class_code", required=false) String type2ClassCode,
			@RequestParam(value="type_class_table", required=false) String typeClassTable,
			@RequestParam(value="type2_class_table", required=false) String type2ClassTable,
			@RequestParam(value="base_date") String baseDate) {
		
        List<Map<String, Object>> items = this.userCodeService.getUserCode(parentCode,baseDate,type,typeClassCode,type2ClassCode,typeClassTable,type2ClassTable);      
        AjaxResult result = new AjaxResult();
        result.data = items;        
		return result;
	}
	
	@GetMapping("/detail")
	public AjaxResult userCodeDetail(
			@RequestParam(value = "id", required = false) Integer id) {
		Map<String, Object> items = this.userCodeService.userCodeDetail(id);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	
	@PostMapping("/save")
	@Transactional
	public AjaxResult saveUserCode(
			@RequestParam(value = "id", required = false) Integer id,
			@RequestParam(value = "Code", required = false) String code,
			@RequestParam(value = "Value", required = false) String value,
			@RequestParam(value = "Type", required = false) String type,
			@RequestParam(value = "Type2", required = false) String type2,
			@RequestParam(value = "Description", required = false) String description,
			@RequestParam(value = "StartDate", required = false) String startDate,
			@RequestParam(value = "EndDate", required = false) String endDate,
			@RequestParam(value = "parent_code", required = false) String parentCode,
			HttpServletRequest request,
			Authentication auth) {
		
		User user = (User)auth.getPrincipal();
		
		AjaxResult result = new AjaxResult();
		
		List<UserCode> ucList =  this.userCodeRepository.findByCodeAndParentIdIsNull(parentCode);
		
		Integer parentId = null;
		if (ucList.size() > 0) {
			parentId = ucList.get(0).getId();
			List<UserCode> check = this.userCodeRepository.findByCodeAndParentId(code,parentId);
			
			if(id != null) {
				if(check.size() > 0) {
					if(check.get(0).getId() == id) {
						check.remove(0);
					}
				}
			}
			
			if (check.size() > 0) {
				result.message = "이미 존재하는 코드입니다.";
				result.success = false;
				return result;
			}
		}
		
		UserCode uc = null;
		
		if (id == null) {
			uc = new UserCode();
			uc.set_order(10000);
		} else {
			uc = this.userCodeRepository.getUserCodeById(id);
		}
		
		Timestamp start = CommonUtil.tryTimestamp(startDate);
		Timestamp end = CommonUtil.tryTimestamp(endDate);
		
		uc.setCode(code);
		uc.setValue(value);
		uc.setType(type);
		uc.setType2(type2);
		uc.setParentId(parentId);
		uc.setDescription(description);
		uc.setStartDate(start);
		uc.setEndDate(end);
		uc.set_audit(user);
		
		uc = this.userCodeRepository.save(uc);
		
		result.data = uc;
		return result;
	}
	
	@PostMapping("/delete")
	public AjaxResult deleteUserCode(@RequestParam("id") Integer id) {
		this.userCodeRepository.deleteById(id);
		AjaxResult result = new AjaxResult();
		return result;
	}
	
	@GetMapping("/relation_data_list")
	public AjaxResult relationDataList(
			@RequestParam(value="table_name2") String tableName2,
			@RequestParam(value="relation_name") String relationName,
			@RequestParam(value="base_id") String baseId) {
		
        List<Map<String, Object>> items = this.userCodeService.relationDataList(tableName2,relationName,baseId);      
        AjaxResult result = new AjaxResult();
        result.data = items;        
		return result;
	}
	
	@PostMapping("/save_relation_data")
	@Transactional
	public AjaxResult saveRelationData(
			@RequestParam(value="base_id") String baseId,
			@RequestParam(value="table_name2") String tableName2,
			@RequestParam(value="relation_name") String relationName,
			@RequestBody @RequestParam("Q") String Q,
			HttpServletRequest request,
			Authentication auth) throws JSONException {
	
		User user = (User)auth.getPrincipal();
		
		AjaxResult result = new AjaxResult();
		
		JSONObject json = new JSONObject(Q);
		
		JSONArray add = json.getJSONArray("modified");
		JSONArray delete = json.getJSONArray("deleted");
		
		for (int i = 0; i < delete.length(); i++) {
			Integer id = null;
			if(delete.getJSONObject(i).has("id")) {
				id = (Integer) delete.getJSONObject(i).get("id");
			}
			
			if (id != null) {
				RelationData rd = this.relationDataRepository.getRelationDataById(id);
				this.relationDataRepository.deleteById(rd.getId());
			}
		}
		
		Integer order = 1000;
		
		result.success = false;
		for (int i = 0; i < add.length(); i++) {
			Integer id = null;
			if(add.getJSONObject(i).has("id")) {
				id = (Integer) add.getJSONObject(i).get("id");
			}
			
			RelationData rd = null;
			if (id != null) {
				rd = this.relationDataRepository.getRelationDataById(id);
			} else {
				order += 1;
				rd = new RelationData();
				rd.setRelationName(relationName);
				rd.setDataPk1(baseId != "" ? Integer.parseInt(baseId) : null);
				rd.setTableName1("user_code");
				rd.setTableName2(tableName2);
				rd.set_order(order);
			}
			Timestamp start = CommonUtil.tryTimestamp(add.getJSONObject(i).get("start_date"));
			Timestamp end = CommonUtil.tryTimestamp(add.getJSONObject(i).get("end_date"));
			
			rd.setDataPk2(Integer.parseInt(add.getJSONObject(i).get("data_pk2").toString())) ;
			rd.setStartDate(start);
			rd.setEndDate(end);
			rd.set_audit(user);
			
			rd = this.relationDataRepository.save(rd);
		}
		result.success = true;
		return result;
	}
	
	@PostMapping("/rela_data_order_save")
	public AjaxResult relaDataOrderSave(
			@RequestParam(value="Q[]") List<String> Q,
			HttpServletRequest request,
			Authentication auth) {
		
		AjaxResult result = new AjaxResult();

		User user = (User)auth.getPrincipal();
				
		 Integer order = 1;
		 
		 for(int i = 0; i < Q.size(); i++) {
			 Integer id = Integer.parseInt(Q.get(i));
			 RelationData rd = this.relationDataRepository.getRelationDataById(id);
			 rd.set_order(order);
			 rd.set_audit(user);
			 order += 1;
			 rd = this.relationDataRepository.save(rd);
			 result.data = rd;
		 }
		 
		 return result;
	}	
}
