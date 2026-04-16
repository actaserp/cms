package mes.domain.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;

import mes.domain.entity.YearVerificationMonthPlan;

public interface YearVerificationMonthPlanRepository extends JpaRepository<YearVerificationMonthPlan, Integer>{

	List<YearVerificationMonthPlan> findByYearVerPlanTargetIdAndDataMonth(Integer id, int month);


	//void deleteByYearVerficationPlanId(Integer yearVerPlanTargetId); 

}