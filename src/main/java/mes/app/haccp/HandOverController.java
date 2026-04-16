package mes.app.haccp;

import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.transaction.Transactional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.security.core.Authentication;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.common.service.FileService;
import mes.app.haccp.service.HandOverService;
import mes.domain.entity.AttachFile;
import mes.domain.entity.BundleHead;
import mes.domain.entity.HandOver;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.AttachFileRepository;
import mes.domain.repository.BundleHeadRepository;
import mes.domain.repository.HandOverRepository;
import mes.domain.services.CommonUtil;
import mes.domain.services.SqlRunner;

@RestController
@RequestMapping("/api/haccp/hand_over")
public class HandOverController {

	@Autowired
	SqlRunner sqlRunner;
	
	@Autowired
	private HandOverService handOverService;
	
	@Autowired
	BundleHeadRepository bundleHeadRepository;
	
	@Autowired
	HandOverRepository handOverRepository;
	
	@Autowired
	AttachFileRepository attachFileRepository;
	
	@Autowired
	private FileService fileService;
	
	// 결재현황 조회
	@GetMapping("/appr_stat")
	public AjaxResult getHandOverApprStatus(
			@RequestParam(value="start_date", required=false) String startDate, 
			@RequestParam(value="end_date", required=false) String endDate, 
			@RequestParam(value="appr_state", required=false) String apprState,
			HttpServletRequest request) {
		
        List<Map<String, Object>> items = this.handOverService.getHandOverApprStatus(startDate,endDate,apprState);     
        AjaxResult result = new AjaxResult();
        result.data = items;        
		return result;
	}
		
	@GetMapping("/read")
	public AjaxResult getHandOver(
			@RequestParam("data_year") String dataYear) {
		
		List<Map<String, Object>> items = this.handOverService.getHandOver(dataYear);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	
	@GetMapping("/read_in")
	public AjaxResult getHandOverIn(
			@RequestParam(value="bh_id", required=false) Integer bh_id, 
			@RequestParam(value="data_date", required=false) String data_date,
			HttpServletRequest request,
			Authentication auth) {
		
		User user = (User)auth.getPrincipal();
		Map<String, Object> items = this.handOverService.getHandOverIn(bh_id, data_date,user);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	
	@PostMapping("/save")
	public AjaxResult saveHandOver(
			@RequestParam(value = "bh_id", required = false) Integer bh_id,
			@RequestParam(value = "title", required = false) String title,
			@RequestParam(value = "data_date", required = false) String data_date,
			@RequestParam(value = "id", required = false) Integer id,
			@RequestParam(value = "StartDate", required = false) String StartDate,
			@RequestParam(value = "EndDate", required = false) String EndDate,
			@RequestParam(value = "FromName", required = false) String FromName,
			@RequestParam(value = "FromDataPk", required = false) Integer FromDataPk,
			@RequestParam(value = "FromTableName", required = false) String FromTableName,
			@RequestParam(value = "FromTel", required = false) String FromTel,
			@RequestParam(value = "ToName", required = false) String ToName,
			@RequestParam(value = "ToDataPk", required = false) Integer ToDataPk,
			@RequestParam(value = "ToTableName", required = false) String ToTableName,
			@RequestParam(value = "ToTel", required = false) String ToTel,
			@RequestParam(value = "Reason", required = false) String Reason,
			@RequestParam(value = "Description", required = false) String Description,
			@RequestParam(value = "fileId", required = false) String file_id,
			HttpServletRequest request,
			Authentication auth) {
		
		AjaxResult result = new AjaxResult();

		User user = (User)auth.getPrincipal();
		
		BundleHead bh = null;
		if (bh_id > 0) {
			bh = this.bundleHeadRepository.getBundleHeadById(bh_id);
		}else {
			bh = new BundleHead();
		}
		bh.setTableName("hand_over");
		bh.setChar1(title);
		bh.setDate1(CommonUtil.tryTimestamp(data_date));
		bh.set_audit(user);
		bh = this.bundleHeadRepository.save(bh);
		
		HandOver ho = null;
		
		if (bh_id > 0) {
			ho = this.handOverRepository.getBySourceDataPk(bh_id);
		} else {
			ho = new HandOver();
			ho.setSourceTableName("bundle_head");
		}
		ho.setStartDate(CommonUtil.trySqlDate(StartDate));
		ho.setEndDate(CommonUtil.trySqlDate(EndDate));
		
		ho.setFromName(CommonUtil.tryString(FromName));
		ho.setFromDataPk(CommonUtil.tryIntNull(FromDataPk));
		ho.setFromTableName(CommonUtil.tryString(FromTableName));
		ho.setFromTel(CommonUtil.tryString(FromTel));
		
		ho.setToName(CommonUtil.tryString(ToName));
		ho.setToDataPk(CommonUtil.tryIntNull(ToDataPk));
		ho.setToTableName(CommonUtil.tryString(ToTableName));
		ho.setToTel(CommonUtil.tryString(ToTel));
		
		ho.setReason(CommonUtil.tryString(Reason));
		ho.setDescription(CommonUtil.tryString(Description));
		ho.setSourceDataPk(bh.getId());
		ho.set_audit(user);
		ho = this.handOverRepository.save(ho);
		
		if ( StringUtils.hasText(file_id) )  {
			
			Integer data_pk = bh.getId();
			String[] fileIdList = file_id.split(",");
			
			for (String fileId : fileIdList) {
				int _id = Integer.parseInt(fileId);
				this.fileService.updateDataPk(_id, data_pk);
			}
		}
				
		result.data = bh.getId();
		
		return result;
		
	}
	
	@Transactional
	@PostMapping("/delete")
	public AjaxResult deleteHandOver(
			@RequestParam(value = "bh_id", required = false) Integer bh_id,
			@RequestParam(value="table_name", required=false) String table_name,
			@RequestParam(value="attach_name", required=false) String attach_name) {
		
		AjaxResult result = new AjaxResult();
		
		this.handOverRepository.deleteBySourceDataPkAndSourceTableName(bh_id, "bundle_head");
		this.bundleHeadRepository.deleteById(bh_id);
		
		List<AttachFile> afList = this.attachFileRepository.getAttachFileByTableNameAndDataPkAndAttachName(table_name,bh_id,attach_name);
		if(afList.size()>0) {
			MapSqlParameterSource paramMap = new MapSqlParameterSource();
			paramMap.addValue("bh_id", bh_id);
			paramMap.addValue("table_name", table_name);
			paramMap.addValue("attach_name", attach_name);
			
			String sql = """
				   delete from attach_file 
				   where "DataPk" = :bh_id 
				   and "TableName" =  :table_name
				   and "AttachName" = :attach_name
				  """;
				
			this.sqlRunner.execute(sql, paramMap);
		}
		
		result.success = true;
		return result;
	}
}
