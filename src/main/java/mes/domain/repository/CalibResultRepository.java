package mes.domain.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import mes.domain.entity.CalibResult;

public interface CalibResultRepository extends JpaRepository<CalibResult, Integer> {
	
	CalibResult getCalibResultById(Integer id);
}
