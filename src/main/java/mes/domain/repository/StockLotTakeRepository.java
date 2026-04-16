package mes.domain.repository;

import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import mes.domain.entity.StockLotTake;

@Repository
public interface StockLotTakeRepository extends JpaRepository<StockLotTake, Integer> {
	
	StockLotTake getStockLotTakeById(Integer id);
	
	Optional<StockLotTake> findByMaterialLotIdAndStoreHouseIdAndState(int materialLotId, int storehouseId, String state);
}
