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
@Table(name="stop_cause")
@Setter
@Getter
@NoArgsConstructor
public class StopCause extends AbstractAuditModel{

	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	int id;
	
	@Column(name = "\"StopCauseName\"")
	String stopCauseName;
	
	@Column(name = "\"PlanYN\"")
	String planYN;
	
	@Column(name = "\"Coverage\"")
	String coverage;
	
	@Column(name = "\"Description\"")
	String description;
	
	@Column(name = "\"StopCauseCode\"")
	String stopCauseCode;
	
}
