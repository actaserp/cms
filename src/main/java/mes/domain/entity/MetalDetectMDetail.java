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
@Table(name="metal_detect_m_detail")
@Setter
@Getter
@NoArgsConstructor
public class MetalDetectMDetail extends AbstractAuditModel{

	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	int id;
	
	@Column(name = "\"MetalDetectMaster_id\"")
	Integer metalDetectMasterId;
	
	@Column(name = "\"TestTarget\"")
	String testTarget;
	
	@Column(name = "\"PiecePosition1\"")
	String piecePosition1;
	
	@Column(name = "\"PiecePosition2\"")
	String piecePosition2;
	
	@Column(name = "\"_order\"")
	Integer order;
	
	@Column(name = "\"_status\"")
	String _status;
	
}
