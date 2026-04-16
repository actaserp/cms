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
@Table(name="test_mast_mat")
@Setter
@Getter
@NoArgsConstructor
public class TestMastMat extends AbstractAuditModel{

	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	Integer id;
	
	@Column(name ="\"Material_id\"")
	Integer materialId;
	
	@Column(name ="\"TestMaster_id\"")
	Integer testMasterId;
}
