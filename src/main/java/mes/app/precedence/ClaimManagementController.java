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
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.common.service.FileService;
import mes.app.precedence.service.ClaimManagementService;
import mes.domain.entity.BundleHead;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.BundleHeadRepository;

@RestController
@RequestMapping("/api/precedence/claim_management")
public class ClaimManagementController {
	
	@Autowired
	BundleHeadRepository bundleHeadRepository;
	
	
	@Autowired
	ClaimManagementService claimManagementService;
	
	@Autowired
	FileService fileService;
	
	
	
	@GetMapping("/read")
	private AjaxResult Result(
			@RequestParam("start_date") String start_date,
			@RequestParam("end_date") String end_date) {
		
		List<Map<String, Object>> items = this.claimManagementService.getList(start_date, end_date);
		
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
		
		Map<String, Object> items = this.claimManagementService.apprList(bhId,data_date,auth);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	
	
	@PostMapping("/save")
	private AjaxResult save(
			@RequestParam(value="bhId", required=false) Integer bhId,
			@RequestParam(value="title", required=false) String title,
			@RequestParam("data_date") String data_date,
			@RequestParam (value="headInfo") String headInfo,
			@RequestParam (value="contents") String contents,
			@RequestParam (value="improvement") String improvement,
			@RequestParam (value="otherMat") String otherMat,
			@RequestParam (value="pickupDate") String pickupDate,
			@RequestParam (value="location") String location,
			@RequestParam(value="fileId", required=false) String file_id,
			Authentication auth,
			HttpServletRequest request) throws JSONException {
		
		User user = (User)auth.getPrincipal();
		
		JSONObject headInfo1 = new JSONObject(headInfo);
		
		AjaxResult result = new AjaxResult();
		
		Timestamp checkDate = Timestamp.valueOf(data_date+ " 00:00:00");
		
		Timestamp pickupDate1 = Timestamp.valueOf(pickupDate+ " 00:00:00");
		
		BundleHead bh = new BundleHead();
		
		if(bhId > 0) {
			bh = this.bundleHeadRepository.getBundleHeadById(bhId);
		}else {
			bh.setDate1(checkDate);
			bh.setTableName("claim_management");
		}
		bh.setChar1(headInfo1.get("Title").toString());
		bh.setText1(contents);
		bh.setChar2(improvement);
		bh.setChar3(otherMat);
		bh.setChar4(location);
		bh.set_audit(user);
		bh.setDate2(pickupDate1);
		this.bundleHeadRepository.save(bh);
		
		
		if (file_id != null && !file_id.isEmpty())  {
			
			Integer data_pk = bhId;
			String[] fileIdList = file_id.split(",");
			
			for (String fileId : fileIdList) {
				int id = Integer.parseInt(fileId);
				this.fileService.updateDataPk(id, data_pk);
			}
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
		
        this.claimManagementService.mstDelete(bhId);
        
        result.success = true;
        
        return result;
	}
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	
	

}
