package mes.app.system;

import java.time.LocalTime;
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

import mes.app.system.service.ShiftService;
import mes.domain.entity.Shift;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.ShiftRepository;

@RestController
@RequestMapping("/api/system/shift")
public class ShiftController {
	
	@Autowired
	ShiftRepository shiftRepository;
	
	@Autowired
	private ShiftService shiftService;
	
	@GetMapping("/read")
	public AjaxResult getShiftList(
			@RequestParam(value="shift_name", required=false) String shift_name,
			@RequestParam(value ="spjangcd") String spjangcd,
			HttpServletRequest request) {
		
		List<Map<String, Object>> items = this.shiftService.getShiftList(shift_name,spjangcd);
   		
        AjaxResult result = new AjaxResult();
        result.data = items;        				
        
		return result;
	}
	
	
	@GetMapping("/detail")
	public AjaxResult getShiftDetail(
			@RequestParam("id") int id,
			HttpServletRequest request) {
		
		Map<String, Object> item = this.shiftService.getShiftDetail(id);
		
        AjaxResult result = new AjaxResult();
        result.data = item;        				
        
		return result;
	}
	
	
	@PostMapping("/save")
	public AjaxResult saveShift(
			@RequestParam(value="id", required=false) Integer id,
			@RequestParam(value="Code") String Code,
			@RequestParam(value="Name") String Name,
			@RequestParam(value="StartTime") String StartTime,
			@RequestParam(value="EndTime") String EndTime,
			@RequestParam(value="Description", required=false) String Description,
			@RequestParam(value ="spjangcd") String spjangcd,
			HttpServletRequest request,
			Authentication auth) {
		
		User user = (User)auth.getPrincipal();
		Shift shift = null;
		AjaxResult result = new AjaxResult();
		
		boolean code_chk = this.shiftRepository.findByCode(Code).isEmpty();
		
		if (id==null) {
			shift = new Shift();
		} else {
			shift = this.shiftRepository.getShiftById(id);
		}
		
		// 코드중복 체크루틴
		if (Code.equals(shift.getCode())==false && code_chk == false) {
			result.success = false;
			result.message="중복된 근무조코드가 존재합니다.";
			return result;
		}
		
		LocalTime stTime = LocalTime.parse(StartTime);
		LocalTime edTime = LocalTime.parse(EndTime);
		
		shift.setCode(Code);
		shift.setName(Name);
		shift.setStartTime(stTime);
		shift.setEndTime(edTime);
		shift.setDescription(Description);
		shift.set_audit(user);
		shift.setSpjangcd(spjangcd);

		
		shift = this.shiftRepository.save(shift);
		
        result.data=shift;
		return result;
	}
	
	@PostMapping("/delete")
	public AjaxResult deleteShift(@RequestParam("id") int id) {
		this.shiftRepository.deleteById(id);
		AjaxResult result = new AjaxResult();
		
		return result;
	}

}
