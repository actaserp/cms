package mes.app.haccp.calib_inst;

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

import mes.app.haccp.calib_inst.service.CalibInstService;
import mes.domain.entity.CalibInstrument;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.CalibInstrumentRepository;

@RestController
@RequestMapping("/api/haccp/calib_inst")
public class CalibInstController {

	@Autowired
	private CalibInstService calibInstService;

	@Autowired
	CalibInstrumentRepository calibInstrumentRepository;

	// 검교정대상 기기 조회
	@GetMapping("/read")
	public AjaxResult getCalibInstList(@RequestParam(value = "calibInstName", required = false) String calibInstName,
			@RequestParam(value = "table_name", required = false) String tableName,
			HttpServletRequest request) {

		List<Map<String, Object>> items = this.calibInstService.getCalibInstList(calibInstName,tableName);
		AjaxResult result = new AjaxResult();
		result.data = items;
		return result;
	}

	// 검교정대상 기기 상세 조회
	@GetMapping("/detail")
	public AjaxResult getCalibInstDetailList(@RequestParam(value = "id", required = false) Integer id,
			HttpServletRequest request) {

		Map<String, Object> items = this.calibInstService.getCalibInstDetailList(id);
		AjaxResult result = new AjaxResult();
		result.data = items;
		return result;
	}

	// 검교정내역 조회
	@GetMapping("/calib_result_list")
	public AjaxResult getCalibResultList(@RequestParam(value = "calib_inst_id", required = false) Integer calib_inst_id,
			HttpServletRequest request) {

		List<Map<String, Object>> items = this.calibInstService.getCalibResultList(calib_inst_id);
		AjaxResult result = new AjaxResult();
		result.data = items;
		return result;
	}

	// 저장
	@PostMapping("/save")
	public AjaxResult saveDocForm(
			@RequestParam(value="id", required=false) Integer id,
			@RequestParam(value="Name", required=false) String Name,
			@RequestParam(value="CalibInstClass", required=false) String CalibInstClass,
			@RequestParam(value="CycleNumber", required=false) Float CycleNumber,
			@RequestParam(value="AuthorizedCalibDate", required=false) String AuthorizedCalibDate,
			@RequestParam(value="SelfCalibDate", required=false) String SelfCalibDate,
			@RequestParam(value="NextCalibtDate", required=false) String NextCalibtDate,
			@RequestParam(value="CalibJudge", required=false) String CalibJudge,
			@RequestParam(value="StartDate", required=false) String StartDate,
			@RequestParam(value="EndDate", required=false) String EndDate,
			@RequestParam(value="SourceDataPk", required=false) Integer SourceDataPk,
			@RequestParam(value="table_name", required=false) String table_name,
			@RequestParam(value="Description", required=false) String Description,
			HttpServletRequest request,
			Authentication auth) {

        
        AjaxResult result = new AjaxResult();
        
		User user = (User)auth.getPrincipal();

        String CycleBase = "M";
        
		CalibInstrument calibInstrument = null;

		if (id == null) {
			calibInstrument = new CalibInstrument();		
		} else {
			calibInstrument = this.calibInstrumentRepository.getCalibInstrumentById(id);
		}

        calibInstrument.setName(Name);
        calibInstrument.setCalibInstClass(CalibInstClass);
        calibInstrument.setCycleBase(CycleBase);
        calibInstrument.setCycleNumber(CycleNumber);
        if (!StringUtils.hasText(AuthorizedCalibDate)) {
        	calibInstrument.setAuthorizedCalibDate(null);
        } else {
        	calibInstrument.setAuthorizedCalibDate(Date.valueOf(AuthorizedCalibDate));
        }
        
        if (!StringUtils.hasText(SelfCalibDate)) {
            calibInstrument.setSelfCalibDate(null);
        } else {
            calibInstrument.setSelfCalibDate(Date.valueOf(SelfCalibDate));
        }
        
        if (!StringUtils.hasText(NextCalibtDate)) {
        	calibInstrument.setNextCalibDate(null);
        } else {
        	calibInstrument.setNextCalibDate(Date.valueOf(NextCalibtDate));
        }
        
        if (!StringUtils.hasText(StartDate)) {
            calibInstrument.setStartDate(null);
        } else {
            calibInstrument.setStartDate(Date.valueOf(StartDate));
        }
        
        if (!StringUtils.hasText(EndDate)) {
            calibInstrument.setEndDate(null);
        } else {
            calibInstrument.setEndDate(Date.valueOf(EndDate));
        }
        calibInstrument.setCalibJudge(CalibJudge);
        calibInstrument.setSourceDataPk(SourceDataPk);
        calibInstrument.setSourceTableName(table_name);
        calibInstrument.setDescription(Description);
		calibInstrument.set_audit(user);

        calibInstrument = this.calibInstrumentRepository.save(calibInstrument);
		
        
        Map<String,Object> item = new HashMap<>();
        item.put("id", calibInstrument.getId());
        
        result.data = item;
        
		return result;
	}

	// 삭제
	@PostMapping("/delete")
	public AjaxResult deleteDocForm(@RequestParam("id") Integer id) {

		if (id != null) {
			this.calibInstrumentRepository.deleteById(id);
		}

		AjaxResult result = new AjaxResult();
		return result;
	}

}
