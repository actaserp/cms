package mes.domain.entity;

import java.sql.Date;

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
@Table(name="rela3_data")
@Setter
@Getter
@NoArgsConstructor
public class Rela3Data extends AbstractAuditModel{

	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	int id;
	
	@Column(name = "\"DataPk1\"")
	Integer dataPk1;
	
	@Column(name = "\"TableName1\"")
	String tableName1;
	
	@Column(name = "\"DataPk2\"")
	Integer dataPk2;
	
	@Column(name = "\"TableName2\"")
	String tableName2;
	
	@Column(name = "\"DataPk3\"")
	Integer dataPk3;
	
	@Column(name = "\"TableName3\"")
	String tableName3;
	
	@Column(name = "\"RelationName\"")
	String relationName;
	
	@Column(name = "\"Char1\"")
	String char1;
	
	@Column(name = "\"Number1\"")
	Float number1;
	
	@Column(name = "\"Date1\"")
	Date date1;
	
	@Column(name = "\"Text1\"")
	String text1;
	
	@Column(name = "\"StartDate\"")
	Date startDate;
	
	@Column(name = "\"EndDate\"")
	Date endDate;
	
	@Column(name = "\"_order\"")
	Integer _order;

}
