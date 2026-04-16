package mes.domain.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;

import mes.domain.entity.CheckResult;

public interface CheckResultRepository extends JpaRepository<CheckResult, Integer> {
	
	CheckResult getCheckResultById(Integer id);

	List<CheckResult> findByCheckMasterId(Integer check_id);

	List<CheckResult> findBySourceDataPk(Integer bhId);
	
	List<CheckResult> findBySourceDataPkOrderByIdAsc(Integer bhId);
	
	CheckResult getCheckResultBySourceDataPk(Integer bhId);
	
	void deleteBySourceDataPk(Integer sourceDataPk);
}
