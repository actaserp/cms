package mes.domain.repository;

import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import mes.domain.entity.TestMethod;

@Repository
public interface TestMethodRepository extends JpaRepository<TestMethod, Integer>{

	
	Optional<TestMethod> getTestMethodByCode(String code);
	
	TestMethod getTestMethodById(int id);
}
