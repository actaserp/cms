package mes.app.haccp.calib_result;

import java.sql.Date;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.common.service.FileService;
import mes.app.haccp.calib_result.service.CalibResultService;
import mes.domain.entity.CalibInstrument;
import mes.domain.entity.CalibResult;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.CalibInstrumentRepository;
import mes.domain.repository.CalibResultRepository;

@RestController
@RequestMapping("/api/haccp/calib_result")
public class CalibResultController {

	@Autowired
	private CalibResultService calibResultService;

	@Autowired
	CalibResultRepository calibResultRepository;

	@Autowired
	CalibInstrumentRepository calibInstrumentRepository;

	@Autowired
	private FileService fileService;
	
	@Autowired
	TransactionTemplate transactionTemplate;
	
	// 검교정결과 상세조회
	@GetMapping("/detail")
	public AjaxResult getCalibResultDetailList(
			@RequestParam(value = "id", required = false) Integer id,
			@RequestParam(value = "table_name", required = false) String table_name,
			HttpServletRequest request) {

		Map<String, Object> items = this.calibResultService.getCalibResultDetailList(id,table_name);
		AjaxResult result = new AjaxResult();
		result.data = items;
		return result;
	}

	// 저장
	@PostMapping("/save")
	public AjaxResult saveCalibResult(
			@RequestParam(value = "id", required = false) Integer id,
			@RequestParam(value = "CalibInstrument_id", required = false) Integer CalibInstrument_id,
			@RequestParam(value = "CalibDate", required = false) Date CalibDate,
			@RequestParam(value = "CalibInstitution", required = false) String CalibInstitution,
			@RequestParam(value = "Difference", required = false) String Difference,
			@RequestParam(value = "CalibJudge", required = false) String CalibJudge,
			@RequestParam(value = "Description", required = false) String Description,
			@RequestParam(value = "file_ids", required = false) String file_ids,
			HttpServletRequest request,
			Authentication auth) {

		AjaxResult result = new AjaxResult();
		
		User user = (User)auth.getPrincipal();
	                
		this.transactionTemplate.executeWithoutResult(status->{
			
			CalibResult cr = null;
		
			if (id != null) {
				cr = this.calibResultRepository.getCalibResultById(id);
			} else {
				cr = new CalibResult();
			}
			
			cr.setCalibInstrumentId(CalibInstrument_id);
	        cr.setCalibDate(CalibDate);
	        cr.setCalibInstitution(CalibInstitution);
	        cr.setDifference(Difference);
	        cr.setCalibJudge(CalibJudge);
	        cr.setDescription(Description);
	        cr.set_audit(user);

			cr = this.calibResultRepository.save(cr);
			
			result.data = cr.getId();
			
			if (StringUtils.hasText(file_ids)) {

				Integer data_pk = cr.getId();
				String[] fileIdList = file_ids.split(",");
				
				for (String fileId : fileIdList) {
					int file_id = Integer.parseInt(fileId);
					this.fileService.updateDataPk(file_id, data_pk);
				}
			}
			
			CalibInstrument c = this.calibInstrumentRepository.getCalibInstrumentById(CalibInstrument_id);
			
			if (c != null) {
				
				if (c.getSelfCalibDate() != null && c.getSelfCalibDate().before(CalibDate)) {
					
					c.setSelfCalibDate(CalibDate);
					c.set_audit(user);
					c = this.calibInstrumentRepository.save(c);
				}
			} 			
		});
		
		return result;
	}

	// 삭제
	@PostMapping("/delete")
	public AjaxResult deleteEquipment(
			@RequestParam("id") Integer id,
			@RequestParam("calib_inst_id") Integer calib_inst_id) {
		
		if (id != null) {

			this.calibResultRepository.deleteById(id);
			this.calibResultService.updateCalibInst(calib_inst_id);
		}
		AjaxResult result = new AjaxResult();
		return result;
	}
}
