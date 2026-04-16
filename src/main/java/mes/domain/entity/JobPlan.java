package mes.domain.entity;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import javax.persistence.*;
import java.sql.Timestamp;
import java.util.Date;

@Entity
@Table(name = "job_plan")
@NoArgsConstructor
@Data
@EqualsAndHashCode(callSuper = false)
public class JobPlan {

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private Long id;

	@ManyToOne(fetch = FetchType.LAZY, optional = false)
	@JoinColumn(name = "head_id", foreignKey = @ForeignKey(name = "fk_job_plan_head"))
	private JobPlanHead head;

	private Integer material_id;
	private Integer qty;
	private String spjangcd;
	private String remark;
}
