package mes.domain.repository;

import mes.domain.entity.TrafficDaily;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.time.LocalDate;
import java.util.List;
import java.util.Optional;

public interface TrafficDailyRepository extends JpaRepository<TrafficDaily, Long> {

    Optional<TrafficDaily> findByServiceAndDate(String service, LocalDate date);

    @Query("SELECT t FROM TrafficDaily t WHERE t.date BETWEEN :startDate AND :endDate ORDER BY t.date ASC, t.service ASC")
    List<TrafficDaily> findByDateBetween(@Param("startDate") LocalDate startDate, @Param("endDate") LocalDate endDate);

    @Query("SELECT t.service, SUM(t.totalBytes), SUM(t.requestCount) " +
            "FROM TrafficDaily t WHERE t.date BETWEEN :startDate AND :endDate " +
            "GROUP BY t.service ORDER BY t.service ASC")
    List<Object[]> findGroupedByService(@Param("startDate") LocalDate startDate,
                                        @Param("endDate") LocalDate endDate
                                        );
}
