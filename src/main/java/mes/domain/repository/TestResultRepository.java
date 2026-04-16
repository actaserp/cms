package mes.domain.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import mes.domain.entity.TestResult;

@Repository
public interface TestResultRepository extends JpaRepository<TestResult, Integer>{

	List<TestResult> findBySourceTableNameAndSourceDataPk(String string, Integer mioId);

	TestResult getTestResultById(int id);
	
	TestResult getTestResultBySourceDataPk(int id);

}
