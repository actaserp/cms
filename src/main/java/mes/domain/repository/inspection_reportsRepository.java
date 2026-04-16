package mes.domain.repository;

import mes.domain.entity.inspection_reports;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface inspection_reportsRepository extends JpaRepository<inspection_reports, Integer> {
}
