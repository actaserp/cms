package mes.domain.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import mes.domain.entity.HaccpProcessItem;

@Repository
public interface HaccpProcessItemRepository extends JpaRepository<HaccpProcessItem, Integer> {

	List<HaccpProcessItem> findByHaccpProcessId(Integer id);
}
