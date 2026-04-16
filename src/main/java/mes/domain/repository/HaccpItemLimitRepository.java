package mes.domain.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import mes.domain.entity.HaccpItemLimit;

@Repository
public interface HaccpItemLimitRepository extends JpaRepository<HaccpItemLimit, Integer> {
	
	List<HaccpItemLimit> findByMaterialIdAndHaccpItemIdAndHaccpProcessId(Integer Material_id, Integer HaccpItem_id, Integer HaccpProcess_id);
	
	HaccpItemLimit getHaccpItemLimitById(Integer id);
}
