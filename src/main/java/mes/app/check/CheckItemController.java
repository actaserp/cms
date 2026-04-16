package mes.app.check;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import io.micrometer.core.instrument.util.StringUtils;
import mes.domain.entity.CheckItem;
import mes.domain.entity.CheckItemResult;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.CheckItemRepository;
import mes.domain.repository.CheckItemResultRepository;
import mes.domain.services.CommonUtil;
import mes.domain.services.DateUtil;
import mes.app.check.service.CheckItemService;
import mes.domain.entity.AttachFile;
import mes.domain.repository.AttachFileRepository;

@RestController
@RequestMapping("/api/check/check_item")
public class CheckItemController {

	@Autowired
	private CheckItemService checkItemService;
	
	@Autowired
	CheckItemRepository checkItemRepository;

	@Autowired
	CheckItemResultRepository checkItemResultRepository;
	
	@Autowired
	AttachFileRepository attachFileRepository;
	
	@GetMapping("/read")
	public AjaxResult getCheckItem(
			@RequestParam("check_master_id") String checkMasterId,
			@RequestParam(value = "check_date", required = false) String checkDate,
			@RequestParam(value = "start_date", required = false) String startDate,
			@RequestParam(value = "end_date", required = false) String endDate) 
	 {
		
        List<Map<String, Object>> items = this.checkItemService.getCheckItem(checkMasterId, checkDate, startDate, endDate);      
        AjaxResult result = new AjaxResult();
        result.data = items;        
		return result;
	}
	
	@GetMapping("/detail")
	public AjaxResult getCheckItemDetail(
			@RequestParam(value = "id", required = false) Integer id) {
		
		Map<String, Object> items = this.checkItemService.getCheckItemDetail(id);
        AjaxResult result = new AjaxResult();
        result.data = items;        
		return result;
	}
	
	@PostMapping("/delete")
	public AjaxResult deleteCheckItem(
			@RequestParam(value = "id", required = false) Integer id) {
		AjaxResult result = new AjaxResult();
		
		List<CheckItemResult> check_item_result = this.checkItemResultRepository.findByCheckItemId(id);

		if (check_item_result != null && check_item_result.size() > 0) {
			result.success = false;
			result.message = "해당 점검이 점검결과에서 사용중입니다.";
			return result;
		}
		
		this.checkItemRepository.deleteById(id);
		return result;
		
	}
	
	@PostMapping("/save")
	public AjaxResult saveCheckItem(
			@RequestParam(value = "id", required = false) Integer id,
			@RequestParam(value = "check_master_id", required = false) Integer checkMasterId,
			@RequestParam(value = "item_name", required = false) String itemName,
			@RequestParam(value = "item_code", required = false) String itemCode,
			@RequestParam(value = "group1", required = false) String group1,
			@RequestParam(value = "group2", required = false) String group2,
			@RequestParam(value = "group3", required = false) String group3,
			@RequestParam(value = "cycle_type", required = false) String cycleType,
			@RequestParam(value = "cycle_value", required = false) String cycleValue,
			@RequestParam(value = "result_type", required = false) String resultType,
			@RequestParam(value = "start_date", required = false) String startDate,
			@RequestParam(value = "end_date", required = false) String endDate,
			@RequestParam(value = "min_value", required = false) String min_value,
			@RequestParam(value = "max_value", required = false) String max_value,
			HttpServletRequest request,
			Authentication auth) {
		
		User user = (User)auth.getPrincipal();
		
		AjaxResult result = new AjaxResult();

		String today = DateUtil.getTodayString();        
        if (StringUtils.isEmpty(startDate)) {
        	startDate = today;
        }

        if (StringUtils.isEmpty(endDate)) {
        	endDate = today;
        }
        
		Timestamp start = CommonUtil.tryTimestamp(startDate);
		Timestamp end = CommonUtil.tryTimestamp(endDate);

		//중복 코드 (ex. null ) 입력 가능하도록 수정
		/*
		if (itemCode != null) {
			List<CheckItem> ci = this.checkItemRepository.findByCode(itemCode);
			if (id != null) {
				if (ci != null && ci.size() > 0 && ci.get(0).getId() == id) {
					ci.remove(0);
				}
			}
			
			if(ci.size() > 0) {
				result.message = "중복된 코드가 존재합니다.";
				result.success = false;
				return result;
			}
		}
		*/
		
		
		CheckItem ci = null;
		if (id != null) {
			ci = this.checkItemRepository.getCheckItemById(id);
		} else {
			ci = new CheckItem();
		}
		
		ci.setCheckMasterId(checkMasterId);
		ci.setName(itemName);
		ci.setCode(itemCode);
		ci.setItemGroup1(group1);
		ci.setItemGroup2(group2);
		ci.setItemGroup3(group3);
		ci.setCycleType(cycleType);
		ci.setCycleValue(CommonUtil.tryIntNull(cycleValue));
		ci.setResultType(resultType);
		ci.setStartDate(start);
		ci.setEndDate(end);
		ci.setMinValue(CommonUtil.tryIntNull(min_value));
		ci.setMaxValue(CommonUtil.tryIntNull(max_value));
		ci.set_audit(user);
		
		ci= this.checkItemRepository.save(ci);
		
		result.data = ci;
		return result;
	}
	
	@PostMapping("/saveIndex")
	public AjaxResult saveIndexCheckItem (
			@RequestBody MultiValueMap<String,Object> Q,
			HttpServletRequest request,
			Authentication auth) {
		
		AjaxResult result = new AjaxResult();

		User user = (User)auth.getPrincipal();
		
	    List<Map<String, Object>> data = CommonUtil.loadJsonListMap(Q.getFirst("Q").toString());
	    
	    Integer index = 1;
	    for (int i = 0; i < data.size(); i++) {
	    	Integer id = Integer.parseInt(data.get(i).get("id").toString());
	    	CheckItem ci = this.checkItemRepository.getCheckItemById(id);
	    	ci.setOrder(index);
	    	ci.set_audit(user);
	    	index = index + 1;
	    	ci = this.checkItemRepository.save(ci);
	    	result.data = ci;
	    }
	    return result;

	}
	
	@PostMapping("/ImgDetail")
	public AjaxResult getImageDetailCheckItem (
			@RequestParam(value = "dataPk", required = false) Integer dataPk) {
		
		Map<String, Object> items = this.checkItemService.getImageDetailCheckItem(dataPk);
        AjaxResult result = new AjaxResult();
        result.data = items;        
		return result;
	}
	
	@GetMapping("/ImgShow")
	public ResponseEntity<Resource> getImageShowCheckItem (
			@RequestParam(value = "dataPk", required = false) Integer dataPk) throws IOException {
		
		ResponseEntity<Resource> items = this.checkItemService.getImageShowCheckItem(dataPk);
        //AjaxResult result = new AjaxResult();
        //result.data = items;        
		return items;
	}
	
	@PostMapping("/ImgSave")
	public AjaxResult saveImageCheckItem (
			@RequestParam(value = "id", required = false) Integer id,
			@RequestParam("file_index") Integer file_index,
			@RequestParam("file_id") Integer file_id,
			@RequestParam("attach_name") String attach_name,
			HttpServletRequest request,
			Authentication auth) {
		
		User user = (User)auth.getPrincipal();
		
		AjaxResult result = new AjaxResult();

		AttachFile af = null;
		if (id != null) {
			af = this.attachFileRepository.getAttachFileByIdAndDataPk(file_id,id);
		} else {
			af = new AttachFile();
		}
		
		if (af != null) {
			af.setId(file_id);
		}
		
		af.setFileIndex(file_index);
		af.setAttachName(attach_name + file_index);
		af.set_audit(user);
		
		af= this.attachFileRepository.save(af);
		
		result.data = af;
		return result;
	}
}
