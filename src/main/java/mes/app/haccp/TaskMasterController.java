package mes.app.haccp;

import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.codehaus.jettison.json.JSONException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.haccp.service.TaskMasterService;
import mes.domain.entity.PropData;
import mes.domain.entity.TaskApprover;
import mes.domain.entity.TaskMaster;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.PropDataRepository;
import mes.domain.repository.TaskApproverRepository;
import mes.domain.repository.TaskMasterRepository;
import mes.domain.services.CommonUtil;

@RestController
@RequestMapping("/api/haccp/task_master")
public class TaskMasterController {
	
	@Autowired
	private TaskMasterService taskMasterService;
	
	@Autowired
	TaskMasterRepository taskMasterRepository;
	
	@Autowired
	TaskApproverRepository taskApproverRepository;
	
	@Autowired
	PropDataRepository propDataRepository;
	
	@GetMapping("/read")
	public AjaxResult getTaskMaster(@RequestParam(value="keyword") String keyword) {
        List<Map<String, Object>> items = this.taskMasterService.getCheckResult(keyword);
        AjaxResult result = new AjaxResult();
        result.data = items;
		return result;
	}
	
	
	@PostMapping("/save")
	public AjaxResult saveTaskMaster(
			@RequestParam(value="id", required=false) Integer id,
			@RequestParam("task_group_code") String taskGroupCode,
			@RequestParam("code") String code,
			@RequestParam("task_name") String taskName,
			@RequestParam("cboWeek") String cboWeek,
			@RequestParam("cboMonthDay") String cboMonthDay,
			@RequestParam("cboYearMonth") String cboYearMonth,
			@RequestParam("cboYearDay") String cboYearDay,
//			@RequestParam("description") String description,
			@RequestParam("cycle_base") String cycleBase,
			@RequestParam("cycle_number") String cycleNumber,
			@RequestParam("writer_group") String writer_group,
			@RequestParam("line1_name") String line1Name,
			@RequestParam("line2_name") String line2Name,
			@RequestParam("line3_name") String line3Name,
			@RequestParam("line4_name") String line4Name,
			@RequestParam("approver1_list") String approver1List,
			@RequestParam("approver2_list") String approver2List,
			@RequestParam("approver3_list") String approver3List,
			@RequestParam("approver4_list") String approver4List,
			@RequestParam(value="noti_yn", required=false) String notiYn,
			@RequestParam("txtQHAdd") String txtQHAdd,
			@RequestParam("noti_before") String notiBefore,
			@RequestParam("menu_link") String menu_link,
			HttpServletRequest request,
			Authentication auth
			) throws JSONException {
		
		User user = (User)auth.getPrincipal();
		
		TaskMaster tm = null;
		
		if (id == null) {
			tm = new TaskMaster();
		} else {
			tm = this.taskMasterRepository.getTaskMasterById(id);
		}
		
		boolean codeChk = this.taskMasterRepository.findByCode(code).isEmpty();
		
		AjaxResult result = new AjaxResult();
		
		if (code.equals(tm.getCode()) == false && codeChk == false) {
			result.success = false;
			result.message = "이미 중복된 코드가 존재합니다.";
			return result;
		}
		tm.setGroupCode(taskGroupCode);
		tm.setCode(code);
		tm.setTaskName(taskName);
//		tm.setDescription(description);
		tm.setCycleBase(cycleBase);
		tm.setCycleNumber(!cycleNumber.isEmpty() ? Float.parseFloat(cycleNumber) : 0);
		tm.setLine1Name(line1Name);
		tm.setLine2Name(line2Name);
		tm.setLine3Name(line3Name);
		tm.setLine4Name(line4Name);
		tm.setNotificationYN(notiYn != null ? notiYn : "");
		tm.setNotificationBefore(!notiBefore.isEmpty() ? Float.parseFloat(notiBefore) : 0);
		tm.setWriterGroupId(!writer_group.isEmpty() ? Integer.parseInt(writer_group) : null);
		tm.set_audit(user);
        
		tm = this.taskMasterRepository.save(tm);
		
		
		PropData prop_menu_link1 = this.propDataRepository.findByDataPkAndTableNameAndCode(id,"task_master","menu_link");
		if(prop_menu_link1 == null) {
			PropData prop_menu_link = new PropData();
			prop_menu_link.setDataPk(id);
			prop_menu_link.setTableName("task_master");
			prop_menu_link.setCode("menu_link");
			prop_menu_link.setChar1(menu_link);
			prop_menu_link.set_audit(user);
			this.propDataRepository.save(prop_menu_link);
		}else {
			prop_menu_link1.setChar1(menu_link);
			prop_menu_link1.set_audit(user);
			this.propDataRepository.save(prop_menu_link1);
		}
		
		
		
		if(cycleBase.equals("W")) {
			PropData List = this.propDataRepository.findByDataPkAndTableNameAndCode(id,"task_master","cycle_date1");
			if(List == null) {
				PropData pd = new PropData();
				pd.setDataPk(id);
				pd.setTableName("task_master");
				pd.setCode("cycle_date1");
				pd.setChar1(cboWeek);
				pd.set_audit(user);
				this.propDataRepository.save(pd);
			}else {
				List.setChar1(cboWeek);
				List.set_audit(user);
				this.propDataRepository.save(List);
			}
		}else if(cycleBase.equals("M")) {
			PropData List = this.propDataRepository.findByDataPkAndTableNameAndCode(id,"task_master","cycle_date1");
			if(List == null) {
				PropData pd = new PropData();
				pd.setDataPk(id);
				pd.setTableName("task_master");
				pd.setCode("cycle_date1");
				pd.setChar1(cboMonthDay);
				pd.set_audit(user);
				this.propDataRepository.save(pd);
			}else {
				List.setChar1(cboMonthDay);
				List.set_audit(user);
				this.propDataRepository.save(List);
			}
		}else if(cycleBase.equals("Q")) {
			String[] txtQHAddList = txtQHAdd.split(",");
			if(txtQHAddList.length > 0) {
				PropData List = this.propDataRepository.findByDataPkAndTableNameAndCode(id,"task_master","cycle_date1");
				if(List == null) {
					PropData pd = new PropData();
					pd.setDataPk(id);
					pd.setTableName("task_master");
					pd.setCode("cycle_date1");
					pd.setChar1(txtQHAddList[0].replace("월","").replace("일","").replace("/","-").replace("",""));
					pd.set_audit(user);
					this.propDataRepository.save(pd);
				}else {
					List.setChar1(txtQHAddList[0].replace("월","").replace("일","").replace("/","-").replace("",""));
					List.set_audit(user);
					this.propDataRepository.save(List);
				}
			}else if(txtQHAddList.length > 1) {
				PropData List = this.propDataRepository.findByDataPkAndTableNameAndCode(id,"task_master","cycle_date2");
				if(List == null) {
					PropData pd = new PropData();
					pd.setDataPk(id);
					pd.setTableName("task_master");
					pd.setCode("cycle_date2");
					pd.setChar1(txtQHAddList[1].replace("월","").replace("일","").replace("/","-").replace("",""));
					pd.set_audit(user);
					this.propDataRepository.save(pd);
				}else {
					List.setChar1(txtQHAddList[1].replace("월","").replace("일","").replace("/","-").replace("",""));
					List.set_audit(user);
					this.propDataRepository.save(List);
				}
			}else if(txtQHAddList.length > 2) {
				PropData List = this.propDataRepository.findByDataPkAndTableNameAndCode(id,"task_master","cycle_date3");
				if(List == null) {
					PropData pd = new PropData();
					pd.setDataPk(id);
					pd.setTableName("task_master");
					pd.setCode("cycle_date3");
					pd.setChar1(txtQHAddList[2].replace("월","").replace("일","").replace("/","-").replace("",""));
					pd.set_audit(user);
					this.propDataRepository.save(pd);
				}else {
					List.setChar1(txtQHAddList[2].replace("월","").replace("일","").replace("/","-").replace("",""));
					List.set_audit(user);
					this.propDataRepository.save(List);
				}
			}else if(txtQHAddList.length > 3) {
				PropData List = this.propDataRepository.findByDataPkAndTableNameAndCode(id,"task_master","cycle_date4");
				if(List == null) {
					PropData pd = new PropData();
					pd.setDataPk(id);
					pd.setTableName("task_master");
					pd.setCode("cycle_date4");
					pd.setChar1(txtQHAddList[3].replace("월","").replace("일","").replace("/","-").replace("",""));
					pd.set_audit(user);
					this.propDataRepository.save(pd);
				}else {
					List.setChar1(txtQHAddList[3].replace("월","").replace("일","").replace("/","-").replace("",""));
					List.set_audit(user);
					this.propDataRepository.save(List);
				}
			}
		}else if(cycleBase.equals("H")){
			String[] txtQHAddList = txtQHAdd.split(",");
			
			if(txtQHAddList.length > 0){
				PropData List = this.propDataRepository.findByDataPkAndTableNameAndCode(id,"task_master","cycle_date1");
				if(List == null) {
					PropData pd = new PropData();
					pd.setDataPk(id);
					pd.setTableName("task_master");
					pd.setCode("cycle_date1");
					pd.setChar1(txtQHAddList[0].replace("월","").replace("일","").replace("/","-").replace("",""));
					pd.set_audit(user);
					this.propDataRepository.save(pd);
				}else {
					List.setChar1(txtQHAddList[0].replace("월","").replace("일","").replace("/","-").replace("",""));
					List.set_audit(user);
					this.propDataRepository.save(List);
				}
			}else if(txtQHAddList.length > 1){
				PropData List = this.propDataRepository.findByDataPkAndTableNameAndCode(id,"task_master","cycle_date2");
				if(List == null) {
					PropData pd = new PropData();
					pd.setDataPk(id);
					pd.setTableName("task_master");
					pd.setCode("cycle_date2");
					pd.setChar1(txtQHAddList[1].replace("월","").replace("일","").replace("/","-").replace("",""));
					pd.set_audit(user);
					this.propDataRepository.save(pd);
				}else {
					List.setChar1(txtQHAddList[1].replace("월","").replace("일","").replace("/","-").replace("",""));
					List.set_audit(user);
					this.propDataRepository.save(List);
				}
			}
		}else if(cycleBase.equals("Y")) {
			PropData List = this.propDataRepository.findByDataPkAndTableNameAndCode(id,"task_master","cycle_date1");
			if(List == null) {
				PropData pd = new PropData();
				pd.setDataPk(id);
				pd.setTableName("task_master");
				pd.setCode("cycle_date1");
				pd.setChar1(cboYearMonth+"-"+cboYearDay);
				pd.set_audit(user);
				this.propDataRepository.save(pd);
			}else {
				List.setChar1(cboYearMonth+"-"+cboYearDay);
				List.set_audit(user);
				this.propDataRepository.save(List);
			}
		}
		
		
		
		
		//기존결재자 삭제 후 저장
		List<TaskApprover> taList = this.taskApproverRepository.findByTaskMasterIdAndLine(id, 1);

		for (int i = 0; i < taList.size(); i++) {
			if (taList != null) {
				this.taskApproverRepository.deleteById(taList.get(i).getId());
				this.taskApproverRepository.flush();
			}
		}
		List<Map<String, Object>> items = CommonUtil.loadJsonListMap(approver1List);
		
		for (int i=0; i<items.size(); i++) {
			Map<String, Object> data = items.get(i);
			TaskApprover ta = new TaskApprover();
			ta.setTaskMasterId(tm.getId());
			ta.setLine(1);
			String strDepartId = String.valueOf(data.get("Depart_id"));
			ta.setDepartId( ("").equals(strDepartId)||strDepartId=="null"||strDepartId==null ? null : Integer.parseInt(strDepartId) ); 
			ta.setUserId(Integer.parseInt(data.get("User_id").toString())); 
			ta.setShift((String) data.get("Shift"));
			ta.set_audit(user);
			this.taskApproverRepository.save(ta);
		}
		
		//기존결재자 삭제 후 저장
		taList = this.taskApproverRepository.findByTaskMasterIdAndLine(id, 2);

		for (int i = 0; i < taList.size(); i++) {
			if (taList != null) {
				this.taskApproverRepository.deleteById(taList.get(i).getId());
				this.taskApproverRepository.flush();
			}
		}
		items = CommonUtil.loadJsonListMap(approver2List);
		
		for (int i=0; i<items.size(); i++) {
			Map<String, Object> data = items.get(i);
			TaskApprover ta = new TaskApprover();
			ta.setTaskMasterId(tm.getId());
			ta.setLine(2);
			String strDepartId = String.valueOf(data.get("Depart_id"));
			ta.setDepartId( ("").equals(strDepartId)||strDepartId=="null"||strDepartId==null ? null : Integer.parseInt(strDepartId) ); 
			ta.setUserId(Integer.parseInt(data.get("User_id").toString())); 
			ta.setShift((String) data.get("Shift"));
			ta.set_audit(user);
			this.taskApproverRepository.save(ta);
		}
		
		//기존결재자 삭제 후 저장
		taList = this.taskApproverRepository.findByTaskMasterIdAndLine(id, 3);

		for (int i = 0; i < taList.size(); i++) {
			if (taList != null) {
				this.taskApproverRepository.deleteById(taList.get(i).getId());
				this.taskApproverRepository.flush();
			}
		}
		items = CommonUtil.loadJsonListMap(approver3List);
		
		for (int i=0; i<items.size(); i++) {
			Map<String, Object> data = items.get(i);
			TaskApprover ta = new TaskApprover();
			ta.setTaskMasterId(tm.getId());
			ta.setLine(3);
			String strDepartId = String.valueOf(data.get("Depart_id"));
			ta.setDepartId( ("").equals(strDepartId)||strDepartId=="null"||strDepartId==null ? null : Integer.parseInt(strDepartId) ); 
			ta.setUserId(Integer.parseInt(data.get("User_id").toString())); 
			ta.setShift((String) data.get("Shift"));
			ta.set_audit(user);
			this.taskApproverRepository.save(ta);
		}

		//기존결재자 삭제 후 저장
		taList = this.taskApproverRepository.findByTaskMasterIdAndLine(id, 4);

		for (int i = 0; i < taList.size(); i++) {
			if (taList != null) {
				this.taskApproverRepository.deleteById(taList.get(i).getId());
				this.taskApproverRepository.flush();
			}
		}
		items = CommonUtil.loadJsonListMap(approver4List);
		
		for (int i=0; i<items.size(); i++) {
			Map<String, Object> data = items.get(i);
			TaskApprover ta = new TaskApprover();
			ta.setTaskMasterId(tm.getId());
			ta.setLine(4);
			String strDepartId = String.valueOf(data.get("Depart_id"));
			ta.setDepartId( ("").equals(strDepartId)||strDepartId=="null"||strDepartId==null ? null : Integer.parseInt(strDepartId) ); 
			ta.setUserId(Integer.parseInt(data.get("User_id").toString())); 
			ta.setShift((String) data.get("Shift"));
			ta.set_audit(user);
			this.taskApproverRepository.save(ta);
		}
		
        result.data = tm.getId();
		return result;
	}
	
	@GetMapping("/detail")
	public AjaxResult getTaskMasterDetail(
			@RequestParam("id") int id) {
		
		Map<String,Object> items = this.taskMasterService.getTaskMasterDetail(id);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	
	@PostMapping("/delete")
	public AjaxResult deleteTaskMaster(
			@RequestParam("id") int id) {
		this.taskMasterRepository.deleteById(id);
		AjaxResult result = new AjaxResult();
		return result;
	}
}
