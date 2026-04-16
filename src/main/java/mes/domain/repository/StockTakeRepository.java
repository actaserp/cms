package mes.domain.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import mes.domain.entity.StockTake;

@Repository
public interface StockTakeRepository extends JpaRepository<StockTake, Integer> {
	
	StockTake getStockTakeById(Integer id);
}
