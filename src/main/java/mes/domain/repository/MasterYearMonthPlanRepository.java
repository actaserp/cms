package mes.domain.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import mes.domain.entity.MasterYearMonthPlan;


@Repository
public interface MasterYearMonthPlanRepository extends JpaRepository<MasterYearMonthPlan, Integer>{

	List<MasterYearMonthPlan> findByDataYearAndDataMonthAndMasterTableId(Integer dataYear, Integer dataMonth, Integer masterId);

}
