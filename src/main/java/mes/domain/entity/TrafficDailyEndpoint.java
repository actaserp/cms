package mes.domain.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.CreationTimestamp;
import javax.persistence.Id;


import javax.persistence.*;
import java.time.LocalDate;
import java.time.LocalDateTime;

@Entity
@Table(name = "traffic_daily_endpoint",
        uniqueConstraints = @UniqueConstraint(columnNames = {"service", "company", "date", "endpoint"}))
@Getter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class TrafficDailyEndpoint {


    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "usage_id")
    private Long usageId;

    @Column(nullable = false, length = 50)
    private String service;

    @Column(nullable = false, length = 50)
    private String company;

    @Column(nullable = false)
    private LocalDate date;

    @Column(nullable = false, length = 255)
    private String endpoint;

    private Long count;

    private Long bytes;

    private Long elapsed;

    @CreationTimestamp
    @Column(name = "created_at", updatable = false)
    private LocalDateTime createdAt;
}
