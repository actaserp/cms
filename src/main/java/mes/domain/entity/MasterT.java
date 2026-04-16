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
@Table(name="master_t")
@Setter
@Getter
@NoArgsConstructor
public class MasterT extends AbstractAuditModel {

	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	int id;
	
	@Column(name = "\"MasterClass\"")
	String masterClass;
	
	@Column(name = "\"Code\"")
	String code;
	
	@Column(name = "\"Name\"")
	String name;
	
	@Column(name = "\"Description\"")
	String description;
	
	@Column(name = "\"Type\"")
	String type;
	
	@Column(name = "\"Type2\"")
	String type2;
	
	@Column(name = "\"Char1\"")
	String char1;
	
	@Column(name = "\"Char2\"")
	String char2;
	
	@Column(name = "\"Number1\"")
	Float number1;
	
	@Column(name = "\"Date1\"")
	Timestamp date1;
	
	@Column(name = "\"Text1\"")
	String text1;
	
	@Column(name = "\"StartDate\"")
	Timestamp startDate;
	
	@Column(name = "\"EndDate\"")
	Timestamp endDate;
	
	@Column(name="\"_order\"")
	Integer _order;
	
	@Column(name = "\"_status\"")
	String _status;
}
