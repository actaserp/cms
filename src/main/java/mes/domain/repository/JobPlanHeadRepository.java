package mes.domain.repository;

import mes.domain.entity.JobPlan;
import mes.domain.entity.JobPlanHead;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface JobPlanHeadRepository extends JpaRepository<JobPlanHead, Long> {
    
}
