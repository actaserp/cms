package mes.domain.entity;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import javax.persistence.*;

@Entity
@Table(name = "job_plan_head")
@NoArgsConstructor
@Data
@EqualsAndHashCode(callSuper = false)
public class JobPlanHead{

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private Long id;

	private String actnm;
	private String stdate;   // 가능하면 LocalDate 권장
	private String eddate;
	private String datetype;
	private String cmcode;
	private String spjangcd;
	private String description;
}