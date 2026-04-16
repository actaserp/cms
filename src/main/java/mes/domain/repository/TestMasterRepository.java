package mes.domain.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import mes.domain.entity.TestMaster;

@Repository
public interface TestMasterRepository extends JpaRepository<TestMaster, Integer> {
	
	List<TestMaster> findByName(String name);
	
	TestMaster getTestMasterById(Integer id);
	
}