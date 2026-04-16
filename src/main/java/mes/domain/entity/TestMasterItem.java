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
@Table(name="test_item_mast")
@Setter
@Getter
@NoArgsConstructor
public class TestMasterItem extends AbstractAuditModel{

	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	Integer id;
	
	@Column(name ="\"_status\"")
	String _status;
	
	@Column(name ="\"RoundDigit\"")
	Integer RoundDigit;
	
	@Column(name ="\"SpecType\"")
	String SpecType;
	
	@Column(name ="\"LowSpec\"")
	Float LowSpec;
	
	@Column(name ="\"UpperSpec\"")
	Float UpperSpec;
	
	@Column(name ="\"SpecText\"")
	String SpecText;
	
	@Column(name ="\"EngSpecText\"")
	String EngSpecText;
	
	@Column(name ="\"TestMethod\"")
	String TestMethod;
	
	@Column(name ="\"EngTestMethod\"")
	String EngTestMethod;
	
	@Column(name ="\"_order\"")
	Integer _order;
	
	@Column(name ="\"TestItem_id\"")
	Integer TestItem_id;
	
	@Column(name ="\"TestMaster_id\"")
	Integer testMasterId;
}
