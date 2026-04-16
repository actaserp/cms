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
@Table(name="pest_control_standard")
@Setter
@Getter
@NoArgsConstructor
public class PestControlStandard extends AbstractAuditModel {

	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	int id;
	
	@Column(name = "\"HaccpAreaClassCode\"")
	String haccpAreaClassCode;
	
	@Column(name = "\"PestClassCode\"")
	String pestClassCode;
	
	@Column(name = "\"SeasonCode\"")
	String seasonCode;
	
	@Column(name = "\"FromCount\"")
	Integer fromCount;
	
	@Column(name = "\"ToCount\"")
	Integer toCount;
	
	@Column(name = "\"ActionContent\"")
	String actionContent;
	
	@Column(name = "\"StartDate\"")
	Timestamp startDate;
	
	@Column(name = "\"EndDate\"")
	Timestamp endDate;
	
	@Column(name = "\"_status\"")
	String _status;
	
}
