package mes.app.inventory;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;
import java.util.Map;

import javax.transaction.Transactional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.inventory.service.MaterialMoveService;
import mes.domain.entity.MaterialInout;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.MatInoutRepository;
import mes.domain.services.CommonUtil;

@RestController
@RequestMapping("/api/inventory/material_move")
public class MaterialMoveController {
	
	@Autowired
	private MaterialMoveService materialMoveService;
	
	@Autowired
	MatInoutRepository matInoutRepository;
	
	@GetMapping("/read")
	public AjaxResult getMaterialMoveList(
			@RequestParam(value="storehouse_id", required = false) Integer storehouse_id,
			@RequestParam(value="material_id", required = false) Integer material_id,
			@RequestParam("spjangcd") String spjangcd,
			@RequestParam(value="keyword", required = false) String keyword) {

        AjaxResult result = new AjaxResult();
        result.data = this.materialMoveService.getMaterialMoveList(storehouse_id, material_id, keyword, spjangcd);
		return result;
	}

	@GetMapping("/read_house")
	public AjaxResult getHouseMoveList(
			@RequestParam(value="storehouse_id", required = false) Integer storehouse_id,
			@RequestParam(value="mat_grp_pk", required = false) Integer mat_grp_pk,
			@RequestParam("spjangcd") String spjangcd,
			@RequestParam(value="keyword", required = false) String keyword) {

		AjaxResult result = new AjaxResult();
		result.data = this.materialMoveService.getHouseMoveList(storehouse_id, mat_grp_pk, keyword, spjangcd);
		return result;
	}
	
	@PostMapping("/material_move")
	@Transactional
	public AjaxResult saveMaterialMove(
			@RequestParam(value="to_storehouse_id", required = true) Integer to_storehouse_id,
			@RequestParam(value="move_list", required = true) String moveItems,
			Authentication auth) {
		User user = (User)auth.getPrincipal();
		AjaxResult result = new AjaxResult();

		// 현재 일자
		LocalDate nowDate = LocalDate.now();
		
		// 현재 시간
		LocalTime nowTime = LocalTime.now();

		List<Map<String, Object>> moveList = CommonUtil.loadJsonListMap(moveItems);

		for (Map<String, Object> map : moveList) {
			int mat_id = (int)map.get("mat_id");
			float moveQty = Float.parseFloat(map.get("move_qty").toString());
			
			// 출고
			MaterialInout mo = new MaterialInout();
			mo.setStoreHouseId(Integer.parseInt(map.get("storehouse_id").toString()));
			mo.setMaterialId(mat_id);
			mo.setInOut("out");
			mo.setOutputType("move_out");
			mo.setOutputQty(moveQty);
			mo.setInoutDate(nowDate);
			mo.setInoutTime(nowTime);
			mo.setDescription("재고이동");
			mo.setState("confirmed");
			mo.set_status("a");
			mo.set_audit(user);
			this.matInoutRepository.save(mo);
			
			// 입고
			MaterialInout mi = new MaterialInout();
			mi.setStoreHouseId(to_storehouse_id);
			mi.setMaterialId(mat_id);
			mi.setInOut("in");
			mi.setInputType("move_in");
			mi.setInputQty(moveQty);
			mi.setInoutDate(nowDate);
			mi.setInoutTime(nowTime);
			mi.setDescription("재고이동");
			mi.setState("confirmed");
			mi.set_status("a");
			mi.set_audit(user);
			this.matInoutRepository.save(mi);
		}

		return result;
	}
	
	@PostMapping("/material_lot_move")
	public AjaxResult saveLotMove(
			@RequestParam(value="to_storehouse_id", required = true) Integer to_storehouse_id,
			@RequestParam(value="lot_list", required = true) String lots ,
			Authentication auth
			) {
		User user = (User)auth.getPrincipal();
		AjaxResult result = new AjaxResult();
		
		LocalDate nowDate = LocalDate.now();
		LocalTime nowTime = LocalTime.now();
		
		List<Map<String, Object>> lotList = CommonUtil.loadJsonListMap(lots);
		for (Map<String, Object> map : lotList){
			int mat_id = (int)map.get("mat_id");
			int mat_lot_id = (int)map.get("mat_lot_id");
			int from_storehouse_id = (int)map.get("storehouse_id");
			
			String lotnumber =(String)map.get("LotNumber");
			float currentStock = Float.parseFloat(map.get("CurrentStock").toString());
			if(from_storehouse_id==to_storehouse_id) {
				continue;
			}

			MaterialInout mio_out = new MaterialInout();
			mio_out.setMaterialId(mat_id);
			mio_out.setStoreHouseId(from_storehouse_id);
			mio_out.setInOut("out");
			mio_out.setLotNumber(lotnumber);
			mio_out.setOutputType("move_out");
			mio_out.setOutputQty(currentStock);
			mio_out.setInoutDate(nowDate);
			mio_out.setInoutTime(nowTime);
			mio_out.setDescription("로트재고이동");
			mio_out.setState("confirmed");
			mio_out.set_status("a");
			mio_out.set_audit(user);
			this.matInoutRepository.save(mio_out);
			
			MaterialInout mio_in = new MaterialInout();
			mio_in.setMaterialId(mat_id);
			mio_in.setStoreHouseId(to_storehouse_id);
			mio_in.setInOut("in");
			mio_in.setInputType("move_in");
			mio_in.setInputQty(currentStock);
			mio_in.setInoutDate(nowDate);
			mio_in.setInoutTime(nowTime);
			mio_in.setDescription("로트재고이동");
			mio_in.setState("confirmed");
			mio_in.set_status("a");
			mio_in.setLotNumber(lotnumber);
			mio_in.set_audit(user);
			this.matInoutRepository.save(mio_in);
			
			this.materialMoveService.updateMaterialLotStorehouse(mat_lot_id, to_storehouse_id);
		}
		
		return result;
	}

	@GetMapping("/mat_lot_list")
	public AjaxResult getMaterialLotList(
			@RequestParam(value="storehouse_id", required = false) Integer storehouse_id,
			@RequestParam(value="material_id", required = false) Integer material_id
			) {
		AjaxResult result = new AjaxResult();		
		result.data = this.materialMoveService.getMaterialLotList(storehouse_id, material_id);
		return result;
	}
}
