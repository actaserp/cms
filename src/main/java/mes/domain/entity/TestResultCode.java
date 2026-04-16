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
@Table(name="test_result_code")
@NoArgsConstructor
@Data
@EqualsAndHashCode(callSuper=false)
public class TestResultCode extends AbstractAuditModel {

	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	Integer id;
	
	@Column(name = "\"ResultCode\"")
	String resultCode;
	
	@Column(name = "\"ResultName\"")
	String resultName;
	
	@Column(name = "\"PassYN\"")
	String passYn;
	
	@Column(name = "\"TestItem_id\"")
	Integer testItemId;
}
