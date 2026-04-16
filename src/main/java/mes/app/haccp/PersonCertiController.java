package mes.app.haccp;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.transaction.Transactional;

import org.codehaus.jettison.json.JSONException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.common.service.FileService;
import mes.app.haccp.service.PersonCertiService;
import mes.domain.entity.PersonCerti;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.PersonCertiRepository;
import mes.domain.services.CommonUtil;

@RestController
@RequestMapping("/api/haccp/person_certi")
public class PersonCertiController {
	
	@Autowired
	public PersonCertiService personCertiService;
	
	@Autowired
	PersonCertiRepository personCertiRepository;
	
	@Autowired
	private FileService fileService;
	
	@GetMapping("/read_person_medical_report")
	public AjaxResult readPersonMedicalReport(
			@RequestParam(value="keyword", required=false) String keyword,
			@RequestParam(value="dept_id", required=false) String deptId) throws JSONException {
		    
		List<Map<String, Object>> items = this.personCertiService.readPersonMedicalReport(keyword,deptId);
        AjaxResult result = new AjaxResult();
        result.data = items;        
		return result;
	}
	
	@GetMapping("/detail")
	public AjaxResult personMedicalDetail(
			@RequestParam(value="id", required=false) Integer id) {
		
        Map<String, Object> items = this.personCertiService.personMedicalDetail(id);      
        AjaxResult result = new AjaxResult();
        result.data = items;        
		return result;
	}
	
	@PostMapping("/save_person_medical_report")
	@Transactional
	public AjaxResult savePersonMedicalReport(
			@RequestParam(value = "id", required = false) Integer id,
			@RequestParam(value = "fileId", required = false) String fileId,
			@RequestParam(value = "TestDate", required = false) String TestDate,
			@RequestParam(value = "PersonName", required = false) String PersonName,
			@RequestParam(value = "NextTestDate", required = false) String NextTestDate,
			@RequestParam(value = "IssueDate", required = false) String IssueDate,
			@RequestParam(value = "ExpireDate", required = false) String ExpireDate,
			@RequestParam(value = "description", required = false) String Description,
			@RequestParam(value = "SourceDataPk", required = false) Integer SourceDataPk,
			@RequestParam(value = "SourceTableName", required = false) String SourceTableName,
			HttpServletRequest request,
			Authentication auth) {
		
		AjaxResult result = new AjaxResult();

		User user = (User)auth.getPrincipal();
		
		Timestamp nextTest = CommonUtil.tryTimestamp(NextTestDate);
		Timestamp issue = CommonUtil.tryTimestamp(IssueDate);
		Timestamp expire = CommonUtil.tryTimestamp(ExpireDate);
		Timestamp test = CommonUtil.tryTimestamp(TestDate);
		
		PersonCerti pc = null;
		if(PersonName != null) {
			List<PersonCerti> pcList = this.personCertiRepository.findByPersonNameAndCertificateCode(PersonName,"person_medical_report");
			if (pcList.size() > 0) {
				pc = pcList.get(0);
			} else {
				pc = new PersonCerti();
				pc.setPersonName(PersonName);
				pc.setCertificateCode("person_medical_report");
			}
		}
		pc.setSourceDataPk(SourceDataPk != null ? SourceDataPk : null);
		pc.setSourceTableName(SourceTableName != null ? SourceTableName : "");
		pc.setTestDate(test);
		pc.setIssueDate(issue);
		pc.setExpireDate(expire);
		pc.setNextTestDate(nextTest);
		pc.setDescription(Description);
		pc.set_audit(user);
		pc = this.personCertiRepository.save(pc);
		
		if (fileId != null && !fileId.isEmpty())  {
			
			Integer data_pk = pc.getId();
			String[] fileIdList = fileId.split(",");
			
			for (String fileIds : fileIdList) {
				int fid = Integer.parseInt(fileIds);
				this.fileService.updateDataPk(fid, data_pk);
			}
		}
		
		return result;
		
	}
	
	@PostMapping("/delete")
	public AjaxResult deletePersonMedicalReport(@RequestParam(value = "id", required = false) Integer id,HttpServletRequest request) {
		AjaxResult result = new AjaxResult();
		
		this.personCertiRepository.deleteById(id);
		
		return result;
	}
	
}
