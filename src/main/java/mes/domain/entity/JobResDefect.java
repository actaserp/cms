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
@Table(name="job_res_defect")
@Setter
@Getter
@NoArgsConstructor
public class JobResDefect extends AbstractAuditModel{

	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	int id;
	
	@Column(name = "\"ProcessOrder\"")
	Integer processOrder;
	
	@Column(name = "\"LotIndex\"")
	Integer lotIndex;
	
	@Column(name = "\"DefectQty\"")
	Float defectQty;
	
	@Column(name = "\"Description\"")
	String description;
	
	@Column(name = "\"DefectType_id\"")
	Integer defectTypeId;
	
	@Column(name = "\"JobResponse_id\"")
	Integer jobResponseId;
	
	@Column(name = "\"DetailDataPk\"")
	Integer detailDataPk;
	
	@Column(name = "\"DetailTableName\"")
	String detailTableName;

	String spjangcd;
}
