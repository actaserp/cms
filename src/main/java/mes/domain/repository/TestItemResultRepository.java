package mes.domain.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import mes.domain.entity.TestItemResult;

@Repository
public interface TestItemResultRepository extends JpaRepository<TestItemResult, Integer>{

	List<TestItemResult> findByTestResultId(int id);

	List<TestItemResult> findBySourceTableNameAndSourceDataPk(String string, Integer jrPk);

	void deleteByTestResultId(Integer trId);
	
	TestItemResult getTestItemResultById(Integer id);

}
