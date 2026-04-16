package mes.domain.repository;

import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;

import mes.domain.entity.HaccpItem;

public interface HaccpItemRepository extends JpaRepository<HaccpItem, Integer> {
	
	HaccpItem getHaccpItemById(Integer id);

	Optional<HaccpItem> findByCode(String item_code);
}
