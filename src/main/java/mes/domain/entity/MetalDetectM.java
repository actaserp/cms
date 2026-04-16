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
@Table(name="metal_detect_m")
@Setter
@Getter
@NoArgsConstructor
public class MetalDetectM extends AbstractAuditModel{

	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	int id;
	
	@Column(name = "\"Code\"")
	String code;
	
	@Column(name = "\"Type\"")
	String type;
	
	@Column(name = "\"Name\"")
	String name;
	
	@Column(name = "\"TestCount\"")
	Integer testCount;
	
	@Column(name = "\"ProductionTestCycle\"")
	Integer productionTestCycle;
	
	@Column(name = "\"TestPiece\"")
	String testPiece;
	
	@Column(name = "\"Description\"")
	String description;
	
	@Column(name = "\"_status\"")
	String _status;
}
