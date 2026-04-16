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
@Table(name="edu_year_target")
@Setter
@Getter
@NoArgsConstructor
public class EduYearTarget extends AbstractAuditModel{

	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	int id;
	
	@Column(name = "\"DataYear\"")
	Integer dataYear;
	
	@Column(name = "\"EduTitle\"")
	String eduTitle;
	
	@Column(name = "\"EduTarget\"")
	String eduTarget;
	
	@Column(name = "\"EduContent\"")
	String eduContent;
	
	@Column(name = "\"Remark\"")
	String remark;
	
	@Column(name = "\"DataPk\"")
	Integer dataPk;
	
	@Column(name = "\"TableName\"")
	String tableName;
	
	@Column(name = "\"_order\"")
	Integer _order;
	
	@Column(name = "\"_status\"")
	String _status;
}
