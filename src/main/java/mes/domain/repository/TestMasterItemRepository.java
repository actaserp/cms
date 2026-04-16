package mes.domain.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;

import mes.domain.entity.TestMasterItem;

public interface TestMasterItemRepository extends JpaRepository<TestMasterItem, Integer>{

	TestMasterItem getTestMasterById(Integer masterId);

	TestMasterItem getTestMasterItemById(Integer id);

	List<TestMasterItem> getByTestMasterId(int testMasterId);
}
