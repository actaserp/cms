package mes.app.inventory;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.transaction.Transactional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.inventory.service.MatStockTakeService;
import mes.domain.entity.StockTake;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.StockTakeRepository;
import mes.domain.services.SqlRunner;

@RestController
@RequestMapping("/api/inventory/mat_stock_take")
public class MatStockTakeController {

	@Autowired
	private MatStockTakeService matStockTakeService;

	@Autowired
	StockTakeRepository stockTakeRepository;

	@Autowired
	SqlRunner sqlRunner;
	
	// 조회
	@GetMapping("/read")
	public AjaxResult getMatStockTakeList(
			@RequestParam(value = "store_house_id", required = false) Integer house_pk,
			@RequestParam(value = "material_type", required = false) String mat_type,
			@RequestParam(value = "material_group", required = false) Integer mat_grp,
			@RequestParam(value = "material_name", required = false) String mat_name,
			@RequestParam("spjangcd") String spjangcd,
			@RequestParam(value = "manage_level", required = false) String manage_level) {
        
		List<Map<String, Object>> items = this.matStockTakeService.getMatStockTakeList(house_pk, mat_type, mat_grp, mat_name, manage_level, spjangcd);

		AjaxResult result = new AjaxResult();
		result.data = items;

		return result;
	}
	
	// 저장
	@PostMapping("/save")
	@Transactional
	public AjaxResult saveMatStockTake(
			@RequestParam(value="mat_pk", required=false) Integer mat_pk,
			@RequestParam(value="house_pk", required=false) Integer house_pk,
			@RequestParam(value="account_stock", required=false) float account_stock,
			@RequestParam(value="real_stock", required=false) float real_stock,
			@RequestParam(value="gap", required=false) float gap,
			@RequestParam(value="description", required=false) String description,
			@RequestParam("spjangcd") String spjangcd,
			HttpServletRequest request,
			Authentication auth) {

        AjaxResult result = new AjaxResult();
        
		User user = (User)auth.getPrincipal();

		String sql = """
				delete from stock_take 
	            where "Material_id" = :mat_pk 
	            and "StoreHouse_id" = :house_pk
	            and "State" = 'taked'
				""";
		
	    MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("mat_pk", mat_pk);
		dicParam.addValue("house_pk", house_pk);
		
	    this.sqlRunner.execute(sql, dicParam);

		// 현재 일자
		LocalDate date = LocalDate.now();
		DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd");
		
		// 현재 시간
		LocalTime time = LocalTime.now();
		DateTimeFormatter timeFormat = DateTimeFormatter.ofPattern("HH:mm");
		
	    StockTake stockTake = new StockTake();
	    
	    stockTake.setMaterialId(mat_pk);
        stockTake.setStoreHouseId(house_pk);
        stockTake.setTakeDate(LocalDate.parse(date.format(dateFormat)));
        stockTake.setTakeTime(LocalTime.parse(time.format(timeFormat)));
        stockTake.setAccountStock(account_stock);
        stockTake.setRealStock(real_stock);
        stockTake.setGap(gap);
        stockTake.setDescription(description);
        stockTake.setTaker_id(user.getId());
        stockTake.setState("taked");
        stockTake.set_audit(user);
		stockTake.setSpjangcd(spjangcd);
        
        stockTake = this.stockTakeRepository.save(stockTake);
        
        result.data = stockTake;
        
		return result;
	}
	
}
