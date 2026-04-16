package mes.domain.repository;

import mes.domain.entity.JobPlan;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface JobPlanRepository extends JpaRepository<JobPlan, Long> {
    void deleteByHead_Id(Long headId);

}
