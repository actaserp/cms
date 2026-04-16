package mes.app.inventory;

import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.transaction.Transactional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.security.core.Authentication;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.inventory.service.MatStockTakeConfirmService;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.StockTakeRepository;
import mes.domain.services.CommonUtil;
import mes.domain.services.SqlRunner;

@RestController
@RequestMapping("/api/inventory/mat_stock_take_confirm")
public class MatStockTakeConfirmController {

	@Autowired
	private MatStockTakeConfirmService matStockTakeConfirmService;

	@Autowired
	StockTakeRepository stockTakeRepository;

	@Autowired
	SqlRunner sqlRunner;
	
	// 조회
	@GetMapping("/read")
	public AjaxResult getMatStockTakeConfirmList(
			@RequestParam(value = "store_house_id", required = false) Integer house_pk,
			@RequestParam("spjangcd") String spjangcd,
			@RequestParam(value = "material_name", required = false) String mat_name) {
        
		List<Map<String, Object>> items = this.matStockTakeConfirmService.getMatStockTakeConfirmList(house_pk, mat_name, spjangcd);

		AjaxResult result = new AjaxResult();
		result.data = items;

		return result;
	}
	
	// 저장
	@PostMapping("/confirm_update")
	@Transactional
	public AjaxResult saveMatStockTakeConfirm(
			@RequestParam MultiValueMap<String,Object> Q,
			HttpServletRequest request,
			Authentication auth) {

        AjaxResult result = new AjaxResult();
        
		User user = (User)auth.getPrincipal();

		List<Map<String, Object>> qItems = CommonUtil.loadJsonListMap(Q.getFirst("Q").toString());
		
		if (qItems.size() == 0) {
			result.success = false;
			return result;
		}
		
		for(int i = 0; i < qItems.size(); i++) {
			Integer pk = Integer.parseInt(qItems.get(i).get("id").toString());
	    				
	    	MapSqlParameterSource paramMap = new MapSqlParameterSource();
			paramMap.addValue("pk", pk);
			paramMap.addValue("user_pk", user.getId());
			
			String sql = """
					update stock_take
                        set "State" = 'confirmed'
                        , "ConfirmDateTime" = now()
                        , "Confirmer_id" =  :user_pk
                        , _modified = now()
                        , _modifier_id = :user_pk
                        where id = :pk
                        and "State" = 'taked'
					  """;
				
			this.sqlRunner.execute(sql, paramMap);
		}
        
        result.success=true;
		return result;
	}
	
}
