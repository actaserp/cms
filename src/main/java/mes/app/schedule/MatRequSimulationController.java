package mes.app.schedule;

import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.schedule.service.MatRequSimulationService;
import mes.domain.model.AjaxResult;

@RestController
@RequestMapping("/api/schedule/simulation")
public class MatRequSimulationController {
	
	@Autowired
	private MatRequSimulationService matRequSimulationService;
	
	//제품별 수주량 만큼 원부자재 소요량의 전체 레벨의 합계를 보여준다
	@GetMapping("/read")
	public AjaxResult getMatRequSimulationList(
			@RequestParam("ids") String ids,
			@RequestParam("qtys") String qtys,
			@RequestParam("srchStartDt") String srchStartDt) {
		
		Map<String, Object> items = this.matRequSimulationService.getMatRequSimulationList(ids,qtys,srchStartDt);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
}
