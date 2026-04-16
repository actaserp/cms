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
@Table(name="year_verification_plan")
@Setter
@Getter
@NoArgsConstructor
public class YearVerificationPlan extends AbstractAuditModel{

	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	int id;
	
	@Column(name = "\"DataYear\"")
	Integer dataYear;
	
	@Column(name = "\"VerificationTarget\"")
	String verificationTarget;
	
	@Column(name = "\"VerificationMethod\"")
	String verificationMethod;
	
	
	@Column(name = "\"_order\"")
	Integer _order;
	
	@Column(name = "\"_status\"")
	String _status;
}
