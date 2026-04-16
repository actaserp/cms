package mes.app.support;

import java.sql.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.common.service.FileService;
import mes.app.support.service.CustomSvcService;
import mes.domain.entity.CustComplain;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.CustComplainRepository;

@RestController
@RequestMapping("/api/support/custom_svc")
public class CustomSvcController {
	
	@Autowired
	private CustomSvcService customSvcService;
	
	@Autowired
	CustComplainRepository custComplainRepository;
	
	@Autowired
	private FileService fileService;
	
	@GetMapping("/read")
	public AjaxResult getCustomSvc(
			@RequestParam(value="startDt", required=false) String startDt,
			@RequestParam(value="endDt", required=false) String endDt,
			@RequestParam(value="combo", required=false) String combo
			) {
		
		List<Map<String, Object>> items = this.customSvcService.getCustomSvc(startDt, endDt, combo);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	
	@GetMapping("/detail")
	private AjaxResult getCustomSvcDetail(
			@RequestParam(value = "id", required = false) Integer id) {
		
		Map<String, Object> items = this.customSvcService.getCustomSvcDetail(id);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	
	@PostMapping("/save")
	public AjaxResult saveCustomSvc(
			@RequestParam(value="id", required=false) Integer id,
			@RequestParam(value="chkName", required=false) String chkName,
			@RequestParam(value="chkDt", required=false) String chkDt,
			@RequestParam(value="cboType", required=false) String cboType,
			@RequestParam(value="cusCnt", required=false) Integer cusCnt,
			@RequestParam(value="finishDt", required=false) String finishDt,
			@RequestParam(value="title", required=false) String title,
			@RequestParam(value="content", required=false) String content,
			@RequestParam(value="srchFileId", required=false) String fileId,
			@RequestParam(value="type", required=false) String state,
			HttpServletRequest request,
			Authentication auth) {
		
		User user = (User)auth.getPrincipal();
		
		AjaxResult result = new AjaxResult();
		
		CustComplain ccp = new CustComplain();
		if (id != null) {
			ccp = this.custComplainRepository.getCustComplainById(id);
		}
		
		if (state.equals("F")) {
			ccp.setCheckState("조치완료");
			ccp.setFinishDate(Date.valueOf(finishDt));
		} else {
			ccp.setCheckState("조치중");
			ccp.setFinishDate(null);
		} 
		
		ccp.setReceiveDate(Date.valueOf(chkDt));
		ccp.setCheckDate(Date.valueOf(chkDt));
		ccp.setCheckName(chkName);
		ccp.setType(cboType);
		ccp.setQty(cusCnt);
		ccp.setTitle(title);
		ccp.setContent(content);
		ccp.set_audit(user);
		
		this.custComplainRepository.save(ccp);
		
		if (StringUtils.hasText(fileId))  {
			
			Integer data_pk = ccp.getId();
			String[] fileIdList = fileId.split(",");
			
			for (String file_id : fileIdList) {
				int fid = Integer.parseInt(file_id);
				this.fileService.updateDataPk(fid, data_pk);
			}
		}
		
		Map<String, Object> item = new HashMap<>();
		item.put("id", ccp.getId());
		
		result.data = item;
		
		return result;
	}
	
	@PostMapping("/delete")
	public AjaxResult deleteCustomSvc(@RequestParam("id") Integer id) {
		
		this.custComplainRepository.deleteById(id);
		AjaxResult result = new AjaxResult();
		return result;
	}
}
