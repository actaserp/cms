package mes.domain.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.CreationTimestamp;

import javax.persistence.*;
import java.time.LocalDate;
import java.time.LocalDateTime;

@Entity
@Table(name = "traffic_daily_usage",
    uniqueConstraints = @UniqueConstraint(columnNames = {"service", "company", "date"})
)
@Getter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TrafficDailyUsage {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, length = 50)
    private String service;

    @Column(nullable = false, length = 50)
    private String company;

    @Column(nullable = false)
    private LocalDate date;

    @Column(name = "total_count")
    private Long totalCount;

    @Column(name = "total_bytes")
    private Long totalBytes;

    @Column(name = "total_mb")
    private Double totalMb;

    @Column(name = "total_gb")
    private Double totalGb;

    @CreationTimestamp
    @Column(name = "created_at", updatable = false)
    private LocalDateTime createdAt;


}
