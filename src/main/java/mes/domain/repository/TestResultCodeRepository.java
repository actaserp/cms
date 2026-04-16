package mes.domain.repository;

import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;

import mes.domain.entity.TestResultCode;

public interface TestResultCodeRepository extends JpaRepository<TestResultCode, Integer> {

	Optional<TestResultCode> findByTestItemIdAndResultCode(int testItemId, String result_code);
	
	TestResultCode getTestResultItemById(Integer id);
}
