package mes.domain.entity;

import java.sql.Timestamp;

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
@Table(name="prod_week_term")
@Setter
@Getter
@NoArgsConstructor
public class ProdWeekTerm extends AbstractAuditModel{

	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	int id;
	
	@Column(name = "\"DataYear\"")
	Integer dataYear;
	
	@Column(name = "\"WeekIndex\"")
	Integer weekIndex;
	
	@Column(name = "\"StartDate\"")
	Timestamp StartDate;
	
	@Column(name = "\"EndDate\"")
	Timestamp EndDate;
	
	@Column(name = "\"State\"")
	String State;
	
	@Column(name = "\"PlanDate\"")
	Timestamp PlanDate;
}
