package mes.domain.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import mes.domain.entity.TestMasterGroup;

@Repository
public interface TestMasterGroupRepository extends JpaRepository<TestMasterGroup, Integer> {
	List<TestMasterGroup> findByName(String name);
	
	TestMasterGroup getTestMasterGroupById(Integer id);
	
}