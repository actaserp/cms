package mes.domain.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import mes.domain.entity.EduYearMonthPlan;

@Repository
public interface EduYearMonthPlanRepository extends JpaRepository<EduYearMonthPlan, Integer>{

	List<EduYearMonthPlan> findByEduYearTargetIdAndDataMonth(Integer id, int month);

	void deleteByEduYearTargetId(Integer eduYearTargetId);

}
