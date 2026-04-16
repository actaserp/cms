package mes.domain.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import mes.domain.entity.YearVerificationPlan;

public interface YearVerificationPlanRepository extends JpaRepository<YearVerificationPlan, Integer> {

	
	YearVerificationPlan getYearVerPlanById(Integer id);
}
