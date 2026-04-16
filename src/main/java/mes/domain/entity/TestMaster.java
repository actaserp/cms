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
@Table(name="test_mast")
@NoArgsConstructor
@Data
@EqualsAndHashCode(callSuper=false)
public class TestMaster extends AbstractAuditModel {
	
	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	Integer id;
	
	@Column(name ="\"Name\"")
	String name;
	
	@Column(name="\"TestType\"")
	String testtype;
	
	@Column(name="\"TestMasterGroup_id\"")
	Integer testmastergroup_id;		
}