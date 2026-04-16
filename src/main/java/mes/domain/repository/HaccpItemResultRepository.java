package mes.domain.repository;

import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import mes.domain.entity.HaccpItemResult;

@Repository 
public interface HaccpItemResultRepository extends JpaRepository<HaccpItemResult, Integer>{
	Optional<HaccpItemResult> getByHaccpTestIdAndHaccpItemId(int ht_id, int item_id);
}
