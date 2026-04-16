package mes.domain.repository;

import mes.domain.entity.TrafficDailyEndpoint;
import org.springframework.data.jpa.repository.JpaRepository;

import java.time.LocalDate;
import java.util.List;
import java.util.Optional;

public interface TrafficDailyEndpointRepository extends JpaRepository<TrafficDailyEndpoint, Long> {

    List<TrafficDailyEndpoint> findByServiceAndCompanyAndDate(
            String service, String company, LocalDate date);

    Optional<TrafficDailyEndpoint> findByServiceAndCompanyAndDateAndEndpoint(
            String service, String company, LocalDate date, String endpoint);

    boolean existsByServiceAndCompanyAndDateAndEndpoint(
            String service, String company, LocalDate date, String endpoint);
}
