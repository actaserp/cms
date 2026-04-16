package mes.app.definition;

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

import mes.app.definition.service.PersonService;
import mes.domain.entity.Person;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.PersonRepository;

@RestController
@RequestMapping("/api/definition/person")
public class PersonController {

	@Autowired
	private PersonRepository personRepository;
	
	@Autowired
	private PersonService personService;
	
	@GetMapping("/read")
	public AjaxResult getPersonList(
			@RequestParam("worker_name") String workerName,
			@RequestParam("workcenter_id") String workcenterId,
			@RequestParam(value ="spjangcd") String spjangcd,
			@RequestParam(value ="searchRtflag", required = false) String searchRtflag,
			@RequestParam(value ="searchDepart", required = false) Integer searchDepart,
			@RequestParam(value ="searchShift", required = false) String searchShift,
    		HttpServletRequest request) {
       
        List<Map<String, Object>> items = this.personService.getPersonList(workerName
				, workcenterId
				, spjangcd
				, searchRtflag
				, searchDepart
				, searchShift);
               		
        AjaxResult result = new AjaxResult();
        result.data = items;        				
        
		return result;
	}
	
	@GetMapping("/detail")
	public AjaxResult getPersonDetail(
			@RequestParam("id") Integer id,
    		HttpServletRequest request) {
       
        Map<String, Object> item = this.personService.getPersonDetail(id);      
   		
        AjaxResult result = new AjaxResult();
        result.data = item;        				
        
		return result;
	}
	
	@PostMapping("/save")
	public AjaxResult savePerson(
			@RequestParam(value="id", required=false) Integer id,
			@RequestParam(value="Code", required=false) String code,
			@RequestParam(value="Name", required=false) String name,
			@RequestParam(value="Depart_id", required=false) Integer departId,
			@RequestParam(value="Description", required=false) String description,
			@RequestParam(value="Factory_id", required=false) Integer factoryId,
			@RequestParam(value="ShiftCode", required=false) String shiftCode,
			@RequestParam(value="WorkCenter_id", required=false) Integer workCenterId,
			@RequestParam(value="WorkHour", required=false) Float workHour,
			@RequestParam(value="work_division", required=false) String work_division,
			@RequestParam(value="jik_id", required=false) String jik_id,
			@RequestParam(value ="spjangcd") String spjangcd,
			@RequestParam(value ="rtdate", required=false) String rtdate,
			@RequestParam(value ="enddate", required=false) String enddate,
			@RequestParam(value ="rtflag", required=false) String rtflag,
			HttpServletRequest request,
			Authentication auth) {
		
		User user = (User)auth.getPrincipal();
		AjaxResult result = new AjaxResult();
		
		Person person = null;
		
		boolean codeChk = this.personRepository.findByCode(code).isEmpty();
		boolean nameChk = this.personRepository.findByName(name).isEmpty();
		
		if (id == null) {
			person = new Person();
		} else {
			person = this.personRepository.getPersonById(id);
		}
		
		if (name.equals(person.getName()) == false && nameChk == false) {
			result.success = false;
			result.message="중복된 이름이 존재합니다.";
			return result;
		}
		
		if (code.equals(person.getCode()) == false && codeChk == false) {
			result.success = false;
			result.message="중복된 사번이 존재합니다.";
			return result;
		}

		if (rtdate != null && !rtdate.isEmpty()) {
			rtdate = rtdate.replaceAll("-", "");  // "2024-06-24" → "20240624"
			person.setRtdate(rtdate);
		}

		if (enddate != null && !enddate.isEmpty()) {
			enddate = enddate.replaceAll("-", "");  // "2024-06-24" → "20240624"
			person.setEnddate(enddate);
		}


		person.setSpjangcd(spjangcd);
		person.setCode(code);
		person.setName(name);
		person.setDepartId(departId);
		person.setDescription(description);
		person.setFactoryId(factoryId);
		person.setShiftCode(shiftCode);
		person.setWorkCenterId(workCenterId);
		person.setWorkHour(workHour);
		person.set_audit(user);
		person.setPersonGroupId(Integer.valueOf(work_division));
		person.setJik_id(jik_id);
		person.setRtflag(rtflag);

		person = this.personRepository.save(person);
		
		result.data = person;
		
		return result;
	}

	@PostMapping("/delete")
	public AjaxResult deletePerson(@RequestParam("id") Integer id) {
		this.personRepository.deleteById(id);
		AjaxResult result = new AjaxResult();
		return result;
	}
}
