package mes.domain.repository;

import mes.domain.entity.TrafficDailyUsage;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.time.LocalDate;
import java.util.Optional;

@Repository
public interface TrafficDailyUsageRepository extends JpaRepository<TrafficDailyUsage, Long> {

    Optional<TrafficDailyUsage> findByServiceAndCompanyAndDate
            (String service, String company, LocalDate date);

    boolean existsByServiceAndCompanyAndDate(
            String service, String company, LocalDate date);
}
