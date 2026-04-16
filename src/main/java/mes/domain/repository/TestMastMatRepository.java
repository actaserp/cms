package mes.domain.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import mes.domain.entity.TestMastMat;

import java.util.List;
import java.util.Optional;

@Repository
public interface TestMastMatRepository extends JpaRepository<TestMastMat, Integer>{

	TestMastMat getTestMastMatById(Integer id);
	Optional<TestMastMat> findByMaterialIdAndTestMasterId(Integer materialId, Integer testMasterId);
	List<TestMastMat> findByMaterialId(Integer materialId);

}
