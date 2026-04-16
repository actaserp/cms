package mes.domain.entity;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import javax.persistence.*;
import java.time.LocalDate;
import java.time.LocalDateTime;

@Entity
@Table(name = "traffic_daily",
    uniqueConstraints = @UniqueConstraint(columnNames = {"service", "date"})
)
@Getter @Setter
@Builder
public class TrafficDaily {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String service; //mes 인지 sport인지
    private LocalDate date;  // 집계 날짜

    @Column(name = "total_bytes")
    private Long totalBytes; //누적 바이트

    @Column(name = "total_gb")
    private Double totalGb;  // Gb 환산

    @Column(name = "request_count")
    private Long requestCount; //총 요청 수

    @Column(name = "recorded_at")
    private LocalDateTime recordedAt;

}
