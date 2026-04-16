package mes.app.haccp;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.transaction.Transactional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.haccp.service.WorkPlaceThService;
import mes.domain.entity.MasterResult;
import mes.domain.entity.MasterT;
import mes.domain.entity.PropData;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.MasterResultRepository;
import mes.domain.repository.MasterTRepository;
import mes.domain.repository.PropDataRepository;
import mes.domain.services.CommonUtil;

@RestController
@RequestMapping("/api/haccp/workplace_th")
public class WorkPlaceThController {

	@Autowired
	private WorkPlaceThService workPlaceThService;
	
	@Autowired
	MasterTRepository masterTRepository;
	
	@Autowired
	PropDataRepository propDataRepository;
	
	@Autowired
	MasterResultRepository masterResultRepository;
	
	@GetMapping("/read_master")
	public AjaxResult getWorkPlaceTh(
			@RequestParam(value="master_class") String masterClass,
			@RequestParam(value="base_date") String baseDate) {
		
        List<Map<String, Object>> items = this.workPlaceThService.getWorkPlaceTh(masterClass,baseDate);      
        AjaxResult result = new AjaxResult();
        result.data = items;        
		return result;
	}
	
	@GetMapping("/detail_master")
	public AjaxResult getWorkPlaceThDetail(
			@RequestParam("id") int id) {
		
		Map<String, Object> items = this.workPlaceThService.getWorkPlaceThDetail(id);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
		
	}
	
	@PostMapping("/save_master")
	@Transactional
	public AjaxResult saveWorkPlaceTh(
			@RequestParam(value = "id", required = false) Integer id,
			@RequestParam(value="Code") String code,
			@RequestParam(value="master_class") String masterClass,
			@RequestParam(value="Description") String description,
			@RequestParam(value="Name") String name,
			@RequestParam(value="StartDate") String startDate,
			@RequestParam(value="EndDate") String endDate,
			@RequestParam(value="humid_code") String humid_code,
			@RequestParam(value="humid_low") String humid_low,
			@RequestParam(value="humid_upper") String humid_upper,
			@RequestParam(value="temp_code") String temp_code,
			@RequestParam(value="temp_low") String temp_low,
			@RequestParam(value="temp_upper") String temp_upper,
			@RequestParam(value="time1") String time1,
			@RequestParam(value="time2") String time2,
			HttpServletRequest request,
			Authentication auth
			) {
		
		User user = (User)auth.getPrincipal();
		
		AjaxResult result = new AjaxResult();
		
		MasterT mt = null;
		
		if (id == null) {
			mt = new MasterT(); 
			mt.set_order(10000);
		} else {
			mt = this.masterTRepository.getMasterTById(id);
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
		mt.setDescription(description != null ? description : "");
		mt.setStartDate(start);
		mt.setEndDate(end);
		mt.set_audit(user);
        
		mt = this.masterTRepository.save(mt);
		
		Integer masterId = mt.getId();
		
	    String[] numPropList = {"temp_low", "temp_upper", "humid_low", "humid_upper"};
	    String[] numList = {temp_low, temp_upper, humid_low, humid_upper};
	    String[] charPropList = {"temp_code", "humid_code", "time1", "time2"};
	    String[] charList = {temp_code, humid_code, time1, time2};
	    
	    for (int i = 0; i < numPropList.length; i++) {
	    	List<PropData> pdList = this.propDataRepository.findByTableNameAndDataPkAndCode("master_t", masterId, numPropList[i]);
	    	PropData pd = null;
	    	if (pdList.size() > 0) {
	    		pd = pdList.get(0);
	    	} else {
	    		pd = new PropData();
	    		pd.setTableName("master_t");
	    		pd.setDataPk(masterId);
	    		pd.setCode(numPropList[i]);
	    	}
	    	pd.set_audit(user);
	    	pd.setNumber1(numList[i] != "" ? Float.parseFloat(numList[i]) : null);
	    	this.propDataRepository.save(pd);
	    }
	    
	    for (int i = 0; i < charPropList.length; i++) {
	    	List<PropData> pdList = this.propDataRepository.findByTableNameAndDataPkAndCode("master_t", masterId, charPropList[i]);
	    	PropData pd = null;
	    	if (pdList.size() > 0) {
	    		pd = pdList.get(0);
	    	} else {
	    		pd = new PropData();
	    		pd.setTableName("master_t");
	    		pd.setDataPk(masterId);
	    		pd.setCode(charPropList[i]);
	    	}
	    	pd.set_audit(user);
	    	pd.setChar1(charList[i]);
	    	this.propDataRepository.save(pd);
	    }
	    
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
	
	@PostMapping("/delete")
	@Transactional
	public AjaxResult deleteWorkPlaceTh(@RequestParam("id") Integer id) {
		this.masterTRepository.deleteById(id);
		this.propDataRepository.deleteByDataPk(id);
		AjaxResult result = new AjaxResult();
		return result;
	}
	
	@GetMapping("/read_result")
	public AjaxResult readResultWorkPlaceTh(
			@RequestParam(value="master_class") String masterClass,
			@RequestParam(value="base_date") String baseDate) {
		
        List<Map<String, Object>> items = this.workPlaceThService.readResultWorkPlaceTh(masterClass,baseDate);      
        AjaxResult result = new AjaxResult();
        result.data = items;        
		return result;
	}
	
	@GetMapping("/detail_result")
	public AjaxResult detailResultWorkPlaceTh(
			@RequestParam(value = "id", required = false) Integer id,
			@RequestParam(value = "master_id", required = false) Integer masterId) {
		
        Map<String, Object> items = this.workPlaceThService.detailResultWorkPlaceTh(id,masterId);      
        AjaxResult result = new AjaxResult();
        result.data = items;        
		return result;
	}
	
	@PostMapping("/save_result")
	public AjaxResult saveResultWorkPlaceTh(
			@RequestParam(value = "id", required = false) Integer id,
			@RequestParam(value = "master_id", required = false) Integer masterId,
			@RequestParam(value = "master_class", required = false) String masterClass,
			@RequestParam(value = "base_date", required = false) String baseDate,
			@RequestParam(value = "Temper", required = false) String temper,
			@RequestParam(value = "Humidity", required = false) String humidity,
			@RequestParam(value = "Description", required = false) String description,
			@RequestParam(value = "DataTime", required = false) String dataTime,
			@RequestParam(value = "DataDate", required = false) String dataDate,
			@RequestParam(value = "master_name", required = false) String masterName,
			HttpServletRequest request,
			Authentication auth) {
		
		AjaxResult result = new AjaxResult();

		User user = (User)auth.getPrincipal();
		
		Integer Number1 = !temper.equals("") && temper != null ? Integer.parseInt(temper) : null;
		Integer Number2 = !humidity.equals("") &&  humidity != null ? Integer.parseInt(humidity) : null;
		
		MasterResult mr = null;
		if (id != null) {
			mr = this.masterResultRepository.getMasterResultById(id);
		} else {
			mr = new MasterResult();
		}
		
		mr.setMasterClass(masterClass != null ? masterClass : "");
		mr.setMasterTableId(masterId != null ? masterId : 0);
		mr.setDataDate(Date.valueOf(dataDate));
        if (dataTime.length() == 5) {
            mr.setDataTime(Time.valueOf(dataTime + ":00"));
        } else {
            mr.setDataTime(Time.valueOf(dataTime));
        }
		mr.setNumber1(Number1 != null ? Number1 : null);
		mr.setNumber2(Number2 != null ? Number2 : null);
		mr.setDescription(description != null ? description : null);
		mr.set_audit(user);
		
		mr = this.masterResultRepository.save(mr);
		
		return result;
	}
	
	@PostMapping("/delete_result")
	public AjaxResult deleteResultWorkPlaceTh(@RequestParam(value = "id", required = false) Integer id) {
		AjaxResult result = new AjaxResult();
		
		this.masterResultRepository.deleteById(id);
		
		return result;
	}
	
	
}
