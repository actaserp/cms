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
@Table(name="test_method")
@Setter
@Getter
@NoArgsConstructor
public class TestMethod  extends AbstractAuditModel{

	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	Integer id;
	
	@Column(name ="\"Code\"")
	String code;

	@Column(name ="\"Name\"")
	String name;
	
	@Column(name ="\"EquipmentGroup_id\"")
	Integer equipmentGroupId;
	
	@Column(name ="\"Description\"")
	String description;
}
