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
@Table(name="bundle_head")
@Setter
@Getter
@NoArgsConstructor
public class BundleHead extends AbstractAuditModel {
	
	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	int id;
	
	@Column(name = "\"TableName\"")
	String tableName;
	
	@Column(name = "\"Char1\"")
	String char1;
	
	@Column(name = "\"Char2\"")
	String char2;
	
	@Column(name = "\"Char3\"")
	String char3;
	
	@Column(name = "\"Char4\"")
	String char4;
	
	@Column(name = "\"Number1\"")
	Float number1;
	
	@Column(name = "\"Date1\"")
	Timestamp date1;
	
	@Column(name = "\"Text1\"")
	String text1;
	
	@Column(name = "\"Date2\"")
	Timestamp date2;
}
