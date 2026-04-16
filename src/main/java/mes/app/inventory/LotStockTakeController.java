package mes.app.inventory;

import java.sql.Date;
import java.sql.Time;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.security.core.Authentication;
import org.springframework.transaction.interceptor.TransactionAspectSupport;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.inventory.service.LotStockTakeService;
import mes.domain.entity.StockLotTake;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.StockLotTakeRepository;
import mes.domain.services.CommonUtil;
import mes.domain.services.DateUtil;
import mes.domain.services.SqlRunner;

@RestController
@RequestMapping("/api/inventory/lot_stock_take")
public class LotStockTakeController {
	
	@Autowired
	SqlRunner sqlRunner;
	
	@Autowired
	LotStockTakeService lotStockTakeService;
	
	@Autowired
	TransactionTemplate transactionTemplate;	
	
	@Autowired
	StockLotTakeRepository stockLotTakeRepository;
	
	@GetMapping("/read")
	public AjaxResult getMaterialStockList(
			@RequestParam(value="mat_type", required=false) String matType,
			@RequestParam(value="mat_grp", required=false) Integer matGroup,
			@RequestParam(value="company_id", required=false) Integer companyId,
			@RequestParam("spjangcd") String spjangcd,
			@RequestParam(value="keyword", required=false) String keyword
			) {
		AjaxResult result = new AjaxResult();
		result.data = this.lotStockTakeService.getMaterialStockList(matType, matGroup, companyId, keyword, spjangcd);
		return result;
	}
	
	@GetMapping("/mat_lot_list")
	public AjaxResult getMaterialLotList(
			@RequestParam(value="material_id", required=true) Integer materialId,
			@RequestParam(value="storehouse_id", required=false) Integer storehouseId
			) {
		AjaxResult result = new AjaxResult();
		result.data = this.lotStockTakeService.getMaterialLotList(materialId, storehouseId);
		return result;
	}
	
	@PostMapping("/save_lot_adjust")
	public AjaxResult saveLotAdjust(
			@RequestParam(value="ml_id", required=true) int ml_id,
			@RequestParam(value="storehouse_id", required=true) int storehouse_id,
			@RequestParam(value="CurrentStock", required=true) float account_stock,
			@RequestParam(value="real_stock", required=true) float real_stock,
			@RequestParam(value="gap", required=true) float gap,
			@RequestParam(value="gap_description", required=false) String description,
			@RequestParam(value="spjangcd", required=false) String spjangcd,
			Authentication auth
			) {
		
		User user = (User)auth.getPrincipal();
		AjaxResult result = new AjaxResult();
		
		Date now = new Date(System.currentTimeMillis());
		
		String stock_time = DateUtil.getHHmmByTodayString() + ":00";
		Time stockTime = Time.valueOf(stock_time);
		
		Optional<StockLotTake> optStockLotTake = this.stockLotTakeRepository.findByMaterialLotIdAndStoreHouseIdAndState(ml_id, storehouse_id, "taked");		
		this.transactionTemplate.executeWithoutResult(status->{
			try {
				
				if(optStockLotTake.isPresent()) {
					StockLotTake oldStockLotTake = optStockLotTake.get();
					this.stockLotTakeRepository.delete(oldStockLotTake);
				}
				
				StockLotTake stockLotTake = new StockLotTake();
				stockLotTake.setMaterialLotId(ml_id);
				stockLotTake.setStoreHouseId(storehouse_id);
				stockLotTake.setAccountStock(account_stock);
				stockLotTake.setRealStock(real_stock);
				stockLotTake.setGap(gap);
				stockLotTake.setTakeDate(now);
				stockLotTake.setTakeTime(stockTime);
				stockLotTake.setDescription(description);
				stockLotTake.setState("taked");
				stockLotTake.set_audit(user);
				stockLotTake.setSpjangcd(spjangcd);
				this.stockLotTakeRepository.save(stockLotTake);
				
				result.data = stockLotTake.getId();
			}
			catch(Exception ex) {
				TransactionAspectSupport.currentTransactionStatus().setRollbackOnly();
				result.success=false;
				result.message = ex.toString();
			}
		});
		
		return result;
	}
	
	@GetMapping("/search_lot")
	public AjaxResult searchLot(
			@RequestParam(value="mat_id", required=false) Integer mat_id,
			@RequestParam(value="storehouse_id", required=false) Integer storehouse_id,
			@RequestParam(value="lot_number", required=true) String lot_number
			) {
		AjaxResult result = new AjaxResult();
		result.data = this.lotStockTakeService.searchLotList(mat_id, storehouse_id, lot_number);
		return result;
	}
	
	//LOT재고조정확인	
	@GetMapping("/lot_adjust_confirm_list")
	public AjaxResult getLotAdjustConfirmList(
			@RequestParam(value="storehouse_id", required=false) Integer storehouse_id,
			@RequestParam(value="keyword", required=false) String keyword,
			@RequestParam("spjangcd") String spjangcd
			) {
		AjaxResult result = new AjaxResult();
		result.data = this.lotStockTakeService.getLotAdjustConfirmList(storehouse_id, keyword, spjangcd);
		return result;
	}
	
	@PostMapping("/confirm_adjust")
	public AjaxResult confirmLotAdjust(@RequestParam(value="ids", required=true) String ids,
			Authentication auth
			) {
		AjaxResult result = new AjaxResult();
		User user = (User)auth.getPrincipal();
		List<Map<String, Object>> id_list = CommonUtil.loadJsonListMap(ids);
		
		for(Map<String, Object> map :id_list) {
			String sql = """
            update stock_lot_take
            set "State" = 'confirmed'
            , "ConfirmDateTime" = now()
            , "Confirmer_id" =  :user_pk
            , _modified = now()
            , _modifier_id = :user_pk
            where id = :id
            and "State" = 'taked'
			""";
			
			int id = (int)(map.get("id"));

			MapSqlParameterSource paramMap = new MapSqlParameterSource();
			paramMap.addValue("user_pk", user.getId());
			paramMap.addValue("id", id);
			
			this.sqlRunner.execute(sql, paramMap);
		}
		result.data = id_list;
		return result;
	}
	
	@GetMapping("/history_list")
	public AjaxResult getSotckLotTakeHistoryList(
			@RequestParam(value="date_from", required=true) String date_from,
			@RequestParam(value="date_to", required=true) String date_to,
			@RequestParam(value="storehouse_id", required=false) Integer storehouse_id,
			@RequestParam(value="mat_type", required=false) String mat_type,
			@RequestParam(value="mat_grp_pk", required=false) Integer mat_grp_pk,
			@RequestParam("spjangcd") String spjangcd,
			@RequestParam(value="keyword", required=false) String keyword
		)	{
		AjaxResult result = new AjaxResult();
		result.data = this.lotStockTakeService.getSotckLotTakeHistoryList(date_from, date_to, storehouse_id, mat_type, mat_grp_pk, keyword, spjangcd);
		return result;
	}
}