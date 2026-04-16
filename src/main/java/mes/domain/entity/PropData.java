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
@Table(name="prop_data")
@Setter
@Getter
@NoArgsConstructor
public class PropData extends AbstractAuditModel {

	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	int id;
	
	@Column(name = "\"DataPk\"")
	Integer dataPk;
	
	@Column(name = "\"TableName\"")
	String tableName;
	
	@Column(name = "\"Code\"")
	String code;
	
	@Column(name = "\"Type\"")
	String type;
	
	@Column(name = "\"Char1\"")
	String char1;
	
	@Column(name = "\"Number1\"")
	Float number1;
	
	@Column(name = "\"Date1\"")
	Timestamp Date1;
	
	@Column(name = "\"Text1\"")
	String text1;
}
