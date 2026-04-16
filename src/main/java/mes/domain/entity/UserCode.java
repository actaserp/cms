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
@Table(name="user_code")
@Setter
@Getter
@NoArgsConstructor
public class UserCode extends AbstractAuditModel{

	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	int id;
	
	@Column(name = "\"Code\"")
	String code;
	
	@Column(name = "\"Value\"")
	String value;

	@Column(name = "\"Description\"")
	String description;
	
	@Column(name = "\"Parent_id\"")
	Integer parentId;
	
	@Column(name = "\"Type\"")
	String type;
	
	@Column(name = "\"Type2\"")
	String type2;
	
	@Column(name = "\"StartDate\"")
	Timestamp startDate;
	
	@Column(name = "\"EndDate\"")
	Timestamp endDate;
		
	@Column(name = "\"_order\"")
	Integer _order;
	
	@Column(name = "\"_status\"")
	String _status;
}
