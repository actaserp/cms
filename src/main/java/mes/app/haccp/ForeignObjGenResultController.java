package mes.app.haccp;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.common.service.FileService;
import mes.app.haccp.service.ForeignObjGenResultService;
import mes.domain.entity.MasterResult;
import mes.domain.entity.MasterT;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.MasterResultRepository;
import mes.domain.repository.MasterTRepository;
import mes.domain.services.CommonUtil;

@RestController
@RequestMapping("/api/haccp/foreign_obj_gen_result")
public class ForeignObjGenResultController {
	
	@Autowired
	public ForeignObjGenResultService foreignObjGenResultService;

	@Autowired
	private FileService fileService;
	
	@Autowired
	MasterTRepository masterTRepository;

	@Autowired
	MasterResultRepository masterResultRepository;
	
	@GetMapping("/read_master")
	public AjaxResult getForeignObj(
			@RequestParam (value="master_class", required=false) String masterClass,
			@RequestParam (value="base_date", required=false) String baseDate) {
		
		List<Map<String, Object>> items = this.foreignObjGenResultService.getForeignObj(masterClass,baseDate);
		AjaxResult result = new AjaxResult();
		result.data = items;
		return result;
	}
	
	@GetMapping("/detail_master")
	public AjaxResult getForeignObjDetail(
			@RequestParam(value = "id", required = false) Integer id,
			@RequestParam(value = "master_id", required = false) Integer masterId) {
		
		Map<String, Object> items = this.foreignObjGenResultService.getForeignObjDetail(id, masterId);
        AjaxResult result = new AjaxResult();
        result.data = items;        
		return result;
	}
	
	@PostMapping("/save_master")
	public AjaxResult saveForeignObj(
			@RequestParam(value="master_id", required = false) Integer masterId,
			@RequestParam(value= "Code" , required=false) String code,
			@RequestParam(value="Description", required = false) String description,
			@RequestParam(value= "master_class", required = false) String masterClass,
			@RequestParam(value= "Name", required = false) String name,
			@RequestParam(value= "Type", required = false) String type,
			@RequestParam(value="Type2", required=false) String type2,
			@RequestParam(value="Char1", required=false) String char1,
			@RequestParam(value= "Number1", required = false) String number1,
			@RequestParam(value="Date1", required=false) String date1,
			@RequestParam(value="Text1", required=false) String text1,
			@RequestParam(value="StartDate", required = false) String startDate,
			@RequestParam(value="EndDate", required = false) String endDate,
			HttpServletRequest request,
			Authentication auth) {
		
		User user = (User)auth.getPrincipal();
		
		AjaxResult result = new AjaxResult();
				
		MasterT mt = null;
		
		if (masterId == null) {
			mt = new MasterT(); 
		} else {
			mt = this.masterTRepository.getMasterTById(masterId);
			mt.set_order(10000);
		}
		
		boolean codeCheck = this.masterTRepository.findByCodeAndMasterClass(code,masterClass).isEmpty();
		
		if (code != null) {
			if (code.equals(mt.getCode()) == false && codeCheck == false) {
				result.message = "이미 존재하는 코드입니다.";
				result.success = false;
				return result;
			}			
		}
		
		Timestamp start = CommonUtil.tryTimestamp(startDate);
		Timestamp end = CommonUtil.tryTimestamp(endDate);
		
		mt.setMasterClass(masterClass != null ? masterClass : "");
		mt.setCode(code != null ? code : "");
		mt.setName(name != null ? name : "");
		mt.setType(type != null ? type : "");
		mt.setType2(type2 != null ? type2 : "");
		mt.setChar1(char1 != null ? char1 : "");
		mt.setNumber1(number1 != null ? Float.parseFloat(number1) : null);
		mt.setDate1(date1 != null ? Timestamp.valueOf(date1) : null);
		mt.setText1(text1 != null ? text1 : "");
		mt.setDescription(description != null ? description : "");
		mt.setStartDate(start);
		mt.setEndDate(end);
		mt.set_audit(user);
        
		mt = this.masterTRepository.save(mt);
		
		result.data = mt;
		return result;
	}
	@PostMapping("/code_order_save")
	public AjaxResult codeOrderSave(
			@RequestParam(value="Q[]") List<String> Q,
			HttpServletRequest request,
			Authentication auth) {
		
		AjaxResult result = new AjaxResult();

		User user = (User)auth.getPrincipal();
				
		 Integer order = 1;
		 
		 for(int i = 0; i < Q.size(); i++) {
			 Integer id = Integer.parseInt(Q.get(i));
			 MasterT mt = this.masterTRepository.getMasterTById(id);
			 mt.set_order(order);
			 mt.set_audit(user);
			 order += 1;
			 mt = this.masterTRepository.save(mt);
			 result.data = mt;
		 }
		 
		 return result;
	}
	
	@PostMapping("/delete_master")
	public AjaxResult deleteMasterT(@RequestParam("id") Integer id) {
		this.masterTRepository.deleteById(id);
		AjaxResult result = new AjaxResult();
		return result;
	}

	// 이물발생내역 조회 
	@GetMapping("/read_result")
	public AjaxResult getForeignObjResult(
			@RequestParam (value="master_class", required=false) String masterClass,
			@RequestParam (value="base_date", required=false) String baseDate) {
		
		List<Map<String, Object>> items = this.foreignObjGenResultService.getForeignObjResult(masterClass,baseDate);
		AjaxResult result = new AjaxResult();
		result.data = items;
		return result;
	}
	
	// 이물발생내역 상세조회 
	@GetMapping("/detail_result")
	public AjaxResult getForeignObjResultDetail(
			@RequestParam(value = "id", required = false) Integer id) {
		
		Map<String, Object> items = this.foreignObjGenResultService.getForeignObjResultDetail(id);
        AjaxResult result = new AjaxResult();
        result.data = items;        
		return result;
	}

	// 저장
	@PostMapping("/save_result")
	public AjaxResult saveForeignObjResult(
			@RequestParam(value="master_class", required = false) String master_class,
			@RequestParam(value="id", required = false) Integer id,
			@RequestParam(value="MasterTable_id", required = false) Integer master_id,
			@RequestParam(value="DataDate" , required=false) String DataDate,
			@RequestParam(value="DataTime" , required=false) String DataTime,
			@RequestParam(value="Char1" , required=false) String Char1,
			@RequestParam(value="Char2" , required=false) String Char2,
			@RequestParam(value="Char3" , required=false) String Char3,
			@RequestParam(value="Char4" , required=false) String Char4,
			@RequestParam(value="Number1" , required=false) Integer Number1,
			@RequestParam(value="Number2" , required=false) Integer Number2,
			@RequestParam(value="Number3" , required=false) Integer Number3,
			@RequestParam(value="Number3" , required=false) Integer Number4,
			@RequestParam(value="Description" , required=false) String Description,
			@RequestParam(value="fileId" , required=false) String file_id,
			HttpServletRequest request,
			Authentication auth) {

		User user = (User)auth.getPrincipal();
		
		AjaxResult result = new AjaxResult();
				
		MasterResult mr = null;
		if (id == null) {
			mr = new MasterResult(); 
		} else {
			mr = this.masterResultRepository.getMasterResultById(id);
		}
			
        mr.setMasterClass(master_class);
        mr.setMasterTableId(master_id);
        mr.setDataDate(DataDate != null ? Date.valueOf(DataDate) : null);
        if (DataTime == null) {
        	mr.setDataTime(null);
        } else if (DataTime.length() == 5) {
            mr.setDataTime(Time.valueOf(DataTime + ":00"));
        } else {
            mr.setDataTime(Time.valueOf(DataTime));
        }
        mr.setChar1(Char1);
        mr.setChar2(Char2);
        mr.setChar3(Char3);
        mr.setChar4(Char4);
        mr.setNumber1(Number1);
        mr.setNumber2(Number2);
        mr.setNumber3(Number3);
        mr.setNumber4(Number4);
        mr.setDescription(Description);
		mr.set_audit(user);
        
		mr = this.masterResultRepository.save(mr);

		if (file_id != null && !file_id.isEmpty())  {
			
			Integer data_pk = mr.getId();
			String[] fileIdList = file_id.split(",");
			
			for (String fileId : fileIdList) {
				int fid = Integer.parseInt(fileId);
				this.fileService.updateDataPk(fid, data_pk);
			}
		}
		
		result.data = mr;
		
		return result;
	}	
	
	// 삭제 
	@PostMapping("/delete_result")
	public AjaxResult deleteResult (@RequestParam("id") Integer id) {

		if (id != null) {
			this.masterResultRepository.deleteById(id);
		}
		AjaxResult result = new AjaxResult();
		return result;
	}

	
}
