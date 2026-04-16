package mes.domain.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import mes.domain.entity.EduYearTarget;

@Repository
public interface EduYearTargetRepository extends JpaRepository<EduYearTarget, Integer>{

	EduYearTarget getEduYearTargetById(Integer eduYearTargetId);

	List<EduYearTarget> findEduYearTargetById(Integer eduYearTargetId);
	
	List<EduYearTarget> findEduYearTargetByDataPk(Integer pk);
}
