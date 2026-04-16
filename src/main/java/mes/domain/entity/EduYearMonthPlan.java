package mes.domain.entity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Entity
@Table(name="edu_year_month_plan")
@Setter
@Getter
@NoArgsConstructor
public class EduYearMonthPlan extends AbstractAuditModel{

	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	int id;
	
	@Column(name = "\"DataMonth\"")
	Integer dataMonth;
	
	@Column(name = "\"PlanYN\"")
	String planYN;
	
	@Column(name = "\"ResultYN\"")
	String resultYN;
	
	@Column(name = "\"EduYearTarget_id\"")
	Integer eduYearTargetId;
	
	@Column(name = "\"_status\"")
	String _status;
}
