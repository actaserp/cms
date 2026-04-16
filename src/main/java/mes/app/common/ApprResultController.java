package mes.app.common;

import java.sql.Timestamp;
import java.text.ParseException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.servlet.http.HttpServletRequest;
import javax.transaction.Transactional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.common.service.ApprResultService;

import mes.domain.entity.ApprResult;
import mes.domain.entity.TaskMaster;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.ApprResultRepository;
import mes.domain.repository.TaskMasterRepository;
import mes.domain.services.DateUtil;

@RestController
@RequestMapping("/api/common/appr_result")
public class ApprResultController {

	@Autowired
	private ApprResultService apprResultService;

	@Autowired
	ApprResultRepository apprResultRepository;
	
	@Autowired
	TaskMasterRepository taskMasterRepository;
	
	
	@GetMapping("/init_appr_box")
	public AjaxResult getInitApprBox(
			@RequestParam(value="head_id", required=false) Integer headId,
			@RequestParam(value="pk", required=false) Integer pk,
			@RequestParam(value="table", required=false) String table,
			@RequestParam(value="task_code", required=false) String taskCode,
			@RequestParam(value="dept_id", required=false) Integer deptId,
			@RequestParam(value="shift", required=false) String shift,
			HttpServletRequest request,
			Authentication auth
			) {
		
		User user = (User)auth.getPrincipal();
		Integer userId = user.getId();
		
		Integer cnt =  this.apprResultService.getApproverCheck(pk, table, headId);
		
		Map<String, Object> apprInfo = new HashMap<String,Object>();
		
		Map<String, Object> dc = new HashMap<String, Object>();
		if (cnt == 0) {
			dc = this.apprResultService.getApproverDefLine(taskCode, deptId, shift);
		} else {
			dc = this.apprResultService.getApproverLine(taskCode, deptId, shift, pk, table, headId, userId);
		}
		
		AjaxResult result = new AjaxResult();
		
		if (dc != null) {
			result.success = true;
		} else {
			result.success = false;
		}
		apprInfo.put("appr_info", dc);
        result.data = apprInfo;        
        
		return result;
	}
	@GetMapping("/appr_box")
	public AjaxResult getApprBoxList(
    		@RequestParam(value="pk", required=false) Integer pk, 
    		@RequestParam(value="table", required=false) String table_name, 
			HttpServletRequest request,
			Authentication auth) {

		User user = (User)auth.getPrincipal();
		
		Map<String, Object> items = this.apprResultService.getApprBoxList(pk, table_name, user.getId());
        AjaxResult result = new AjaxResult();
        result.data = items;        
		return result;
	}
	
	@PostMapping("/approve")
	public AjaxResult saveCheckResult(
			@RequestParam(value = "ar_id", required = false) Integer ar_id,
			@RequestParam(value = "appr_state", required = false) String appr_state,
			@RequestParam(value = "description", required = false) String description,
			HttpServletRequest request,
			Authentication auth) throws ParseException {
		
		AjaxResult result = new AjaxResult();

		User user = (User)auth.getPrincipal();
		
		Integer user_id = user.getId();
		Timestamp this_time = DateUtil.getNowTimeStamp();
		
		
		ApprResult apprResult = this.apprResultRepository.getApprResultById(ar_id);
		
		if (apprResult != null) {
			
			apprResult.setApproverId(user_id);
			apprResult.setApprDate(this_time);
			apprResult.setState(appr_state);
			apprResult.setState("Y");
			apprResult.setDescription(description);
			
			apprResult.set_audit(user);
			
			apprResult = this.apprResultRepository.save(apprResult);
		}
		
		return result;
	}

	@GetMapping("/to_approve_list")
	public AjaxResult getToApproveList(
			HttpServletRequest request,
			Authentication auth) {

		User user = (User)auth.getPrincipal();
		
		List<Map<String, Object>> items = this.apprResultService.getToApproveList(user.getId());
        AjaxResult result = new AjaxResult();
        result.data = items;        
		return result;
	}
	@PostMapping("/req_approve")
	public AjaxResult ReqApprove(
			@RequestParam(value = "task_code", required = false) String taskCode,
			@RequestParam(value = "pk", required = false) Integer pk,
			@RequestParam(value = "table", required = false) String table,
			@RequestParam(value = "link_gui", required = false) String linkGui,
			@RequestParam(value = "link_title", required = false) String linkTitle,
			@RequestParam(value = "link_gui_param", required = false) String linkGuiParam,
			@RequestParam(value = "approver_list", required = false) String approverList,
			HttpServletRequest request,
			Authentication auth) {
		
        AjaxResult result = new AjaxResult();
        
        User user = (User)auth.getPrincipal();
        
        Integer userId = user.getId();
        
        boolean ret = false;
        
        ret = this.apprResultService.ApproverHeadInsert(taskCode,pk,table,userId,linkTitle,linkGui,linkGuiParam, user);
        
        if (ret) {
        	ret = this.apprResultService.ApprItemInsert(taskCode,pk,table,userId,approverList.split(","), user);
        }
        
        result.success = ret;
        
		return result;
	}
	
	
	@PostMapping("/insert")
	@Transactional
	public AjaxResult ApproveInsert(
			@RequestParam(value = "pk", required = false) Integer pk,
			@RequestParam(value = "table", required = false) String table,
			@RequestParam(value = "task_code", required = false) String taskCode,
			HttpServletRequest request,
			Authentication auth) {
		
		AjaxResult result = new AjaxResult();

		User user = (User)auth.getPrincipal();
		
		Optional<TaskMaster> tm = this.taskMasterRepository.findByCode(taskCode);
		
		Integer userId = user.getId();
		Timestamp time = DateUtil.getNowTimeStamp();
		
		ApprResult ar = new ApprResult();
		ar.setSourceDataPk(pk);
		ar.setSourceTableName(table);
		ar.setLine(0);
		ar.setLineName("작성");
		ar.setApproverId(userId);
		ar.setApprDate(time);
		ar.setState("Y");
		ar.set_audit(user);
		
		ar = this.apprResultRepository.save(ar);
		
		if (!tm.isEmpty()) {
			if(!tm.get().getLine1Name().isEmpty() && tm.get().getApprover1Id() != null) {
				ApprResult ar1 = new ApprResult();
				ar1.setSourceDataPk(pk);
				ar1.setSourceTableName(table);
				ar1.setLine(1);
				ar1.setLineName(tm.get().getLine1Name());
				ar1.setApproverId(tm.get().getApprover1Id());
				ar1.set_audit(user);
				
				ar1 = this.apprResultRepository.save(ar1);
			}
			
			if(!tm.get().getLine2Name().isEmpty() && tm.get().getApprover2Id() != null) {
				ApprResult ar2 = new ApprResult();
				ar2.setSourceDataPk(pk);
				ar2.setSourceTableName(table);
				ar2.setLine(2);
				ar2.setLineName(tm.get().getLine2Name());
				ar2.setApproverId(tm.get().getApprover2Id());
				ar2.set_audit(user);
				
				ar2 = this.apprResultRepository.save(ar2);
			}
		}
		return result;
	}
	
	@PostMapping("/conf_approve")
	public AjaxResult confirmApprove(
			@RequestParam(value = "pk", required = false) Integer pk,
			@RequestParam(value = "table", required = false) String table,
			@RequestParam(value = "approval", required = false) String approval,
			@RequestParam(value = "desc", required = false) String desc,
			HttpServletRequest request,
			Authentication auth) {
		
        AjaxResult result = new AjaxResult();
        
        User user = (User)auth.getPrincipal();
                        		
        boolean ret = false;
        
        ret = this.apprResultService.confirmApprove(pk,table,user,approval,desc);
        
        result.success = ret;
        
		return result;
	}
	
	
	@GetMapping("/my_approve_list")
	public AjaxResult myApproveList(
			@RequestParam(value = "from_date", required = false) String from_date,
			@RequestParam(value = "to_date", required = false) String to_date,
			@RequestParam(value = "tm_id", required = false) String tm_id,
			@RequestParam(value = "state", required = false) String state,
			HttpServletRequest request,
			Authentication auth) {
		
        AjaxResult result = new AjaxResult();
        
        User user = (User)auth.getPrincipal();
                        		
        List<Map<String, Object>> items = this.apprResultService.myApproveList(from_date,to_date,tm_id,state,user);
        
		result.data = items;
		return result;
	}
	
	
	
	
	
	
	
	
	
	
}
