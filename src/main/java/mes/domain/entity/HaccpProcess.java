package mes.domain.entity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Entity
@Table(name="haccp_proc")
@NoArgsConstructor
@Data
@EqualsAndHashCode(callSuper=false)
public class HaccpProcess extends AbstractAuditModel {
	
	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	int id;	

	@Column(name = "\"Code\"")
	String code;
	
	@Column(name = "\"Name\"")
	String name;

	@Column(name = "\"Description\"")
	String description;

	@Column(name = "\"MonitoringMethod\"")
	String monitoringMethod;

	@Column(name = "\"ActionMethod\"")
	String actionMethod;

	@Column(name = "\"TestCycle\"")
	String testCycle;
	
	@Column(name = "\"ProcessKind\"")
	String processKind;

	@Column(name = "\"Standard\"")
	String standard;

}
