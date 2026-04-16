package mes.app.production;

import java.text.ParseException;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.security.core.Authentication;
import org.springframework.transaction.interceptor.TransactionAspectSupport;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.production.service.ProdPrepareService;
import mes.domain.entity.JobRes;
import mes.domain.entity.MaterialProcessInputRequest;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.JobResRepository;
import mes.domain.repository.MaterialProcessInputRequestReposotory;
import mes.domain.repository.MaterialRepository;
import mes.domain.repository.StorehouseRepository;
import mes.domain.services.CommonUtil;
import mes.domain.services.SqlRunner;

@RestController
@RequestMapping("/api/production/prod_prepare")
public class ProdPrepareController {

	@Autowired
	private ProdPrepareService prodPrepareService;

	@Autowired
	JobResRepository jobResRepository;

	@Autowired
	StorehouseRepository storehouseRepository;

	@Autowired
	MaterialRepository materialRepository;

	@Autowired
	MaterialProcessInputRequestReposotory materialProcessInputRequestReposotory;
	
	@Autowired
	TransactionTemplate transactionTemplate;

	@Autowired
	SqlRunner sqlRunner;
	
	// 작업지시내역 - 리스트 조회
	@GetMapping("/job_order_list")
	public AjaxResult jobOrderSearch(
    		@RequestParam(value="data_date", required=false) String data_date,
    		@RequestParam(value="shift_code", required=false) String shift_code,
    		@RequestParam(value="workcenter_pk", required=false) Integer workcenter_pk,
			@RequestParam("spjangcd") String spjangcd,
			HttpServletRequest request) {
		
        List<Map<String, Object>> items = this.prodPrepareService.jobOrderSearch(data_date, shift_code, workcenter_pk, spjangcd);
        AjaxResult result = new AjaxResult();
        result.data = items;        
		return result;
	}
	
	// 작업지시내역 - 순서저장
	@PostMapping("/save_work_index")
	public AjaxResult saveWorkIndex(
			@RequestParam(value = "to_storehouse_id", required = false) Integer to_storehouse_id,
			@RequestBody MultiValueMap<String,Object> dataList,
			HttpServletRequest request,
			Authentication auth) throws ParseException {

		AjaxResult result = new AjaxResult();

		User user = (User)auth.getPrincipal();
		
	    List<Map<String, Object>> items = CommonUtil.loadJsonListMap(dataList.getFirst("Q").toString());

		if (items.size() == 0) {
			result.success = false;
			return result;
		}
		
		Integer index = 1;
		
		for (int i = 0; i < items.size(); i++) {
			
			Integer pk = Integer.parseInt(items.get(i).get("id").toString());
			//Integer work_index = index;
			
			JobRes jr = this.jobResRepository.getJobResById(pk);
			
			if(jr != null) {
				jr.setWorkIndex(index);
				jr.set_audit(user);
				jr = this.jobResRepository.save(jr);
			}			
			
			index = index + 1;
		}
		
		return result;
	}
	
	// 해당소요자재탭 - 소요량 조회
	@GetMapping("/bom_detail_list")
	public AjaxResult bomDetailList(
    		@RequestParam(value="jr_pks", required=false) String jr_pks,
    		@RequestParam(value="data_date", required=false) String data_date,
			HttpServletRequest request) {
		
        List<Map<String, Object>> items = this.prodPrepareService.bomDetailList(jr_pks, data_date);
        AjaxResult result = new AjaxResult();
        result.data = items;        
		return result;
	}
	
	// 해당소요자재탭 - 재자 공정 투입 요청
	@PostMapping("/save_mat_proc_input")
	public AjaxResult saveMatProcInput(
			@RequestParam(value = "jobres_pks", required = false) String jobres_pks,
			@RequestParam(value = "mat_pks", required = false) String mat_pks,
			@RequestParam(value = "input_req_qtys", required = false) String input_req_qtys,
    		@RequestParam(value="data_date", required=false) String data_date,
			@RequestBody MultiValueMap<String,Object> move_list,
			HttpServletRequest request,
			Authentication auth) throws ParseException {
		
		AjaxResult result = new AjaxResult();

		User user = (User)auth.getPrincipal();
		
		// 1.공정창고가 설정되었는지 체크
		Integer proc_storehouse_count = this.storehouseRepository.countByHouseType("process");
		if (proc_storehouse_count == 0) {
			result.success = false;
			result.message = "공정창고로 지정된 창고가 없습니다.";
			return result;
		}
		
		// 2.넘어온 품목중 창고지정이 안되어 있는 품목이 있는지
		String[] arrMats = mat_pks.split(",");

		for (String id : arrMats) {
			Integer storehouse_nullcount = this.materialRepository.countByIdAndStoreHouseIdIsNull(Integer.parseInt(id));

			if (storehouse_nullcount > 0) {				
				result.success = false;
				result.message = "창고지정이 안되어 있는 품목이 있습니다.";
				return result;				
			}
		}
				
		// mat_proc_input_req, mat_proc_input, job_Res 저장을 위한 [
		this.transactionTemplate.executeWithoutResult(status->{	
			try {
				MaterialProcessInputRequest mpReq = new MaterialProcessInputRequest();
				mpReq.setRequestDate(CommonUtil.tryTimestamp(data_date));
				mpReq.setRequesterId(user.getId());
		        mpReq.set_audit(user);
		        mpReq = materialProcessInputRequestReposotory.save(mpReq);
		        
				Integer req_pk = mpReq.getId();
		        String[] arr_jobres_pk = jobres_pks.split(",");

				for (String pk : arr_jobres_pk) {
					JobRes jr = this.jobResRepository.getJobResById(Integer.parseInt(pk));
					
					if (jr != null) {						
						jr.setMaterialProcessInputRequestId(req_pk);
						jr.set_audit(user);
						
						this.jobResRepository.save(jr);
					}
				}

				MapSqlParameterSource paramMap = new MapSqlParameterSource();
				paramMap.addValue("req_pk",req_pk);
				paramMap.addValue("mat_pks", mat_pks);
				paramMap.addValue("req_qtys", input_req_qtys);
				paramMap.addValue("user_id", user.getId());
								
				String sql = """
						insert into mat_proc_input("MaterialProcessInputRequest_id", "Material_id", "RequestQty", "MaterialStoreHouse_id", "ProcessStoreHouse_id", "State", _created, _creater_id)
	                    with A as (
	                        select unnest(string_to_array(:mat_pks, ','))::int as mat_pk
	                        , unnest(string_to_array(:req_qtys, ','))::float as requ_qty
	                    ), B as (
	                        select id as proc_house_pk 
	                        from store_house sh 
	                        where "HouseType" = 'process'
	                        limit 1
	                    )
	                    select :req_pk, A.mat_pk, A.requ_qty, m."StoreHouse_id", B.proc_house_pk, 'requested', now(), :user_id
	                    from A
	                    inner join material M on M.id = A.mat_pk
	                    inner join B on 1 = 1
					    """;
				
			    this.sqlRunner.execute(sql, paramMap);
			    
			    result.data = req_pk;
			} catch(Exception ex) {
				TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
				result.success=false;
				result.message = ex.toString();
			}
		});
				
		return result;
	}	
	
	// 투입요청내역 조회
	@GetMapping("/mat_proc_input_list")
	public AjaxResult matProcInputList(
    		@RequestParam(value="req_pk", required=false) Integer req_pk,
			HttpServletRequest request) {
		
        List<Map<String, Object>> items = this.prodPrepareService.matProcInputList(req_pk);
        AjaxResult result = new AjaxResult();
        result.data = items;        
		return result;
	}
	
}
