package mes.domain.repository;

import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import mes.domain.entity.TestItem;

@Repository
public interface TestItemRepository extends JpaRepository<TestItem, Integer> {

	Optional<TestItem> findByCode(String code);
	
	TestItem getTestItemById(Integer id);

	Optional<TestItem> findByName(String itemName);
}
