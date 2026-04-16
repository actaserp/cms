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
@Table(name="haccp_proc_item")
@NoArgsConstructor
@Data
@EqualsAndHashCode(callSuper=false)
public class HaccpProcessItem extends AbstractAuditModel {
	
	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	int id;

	@Column(name = "\"_order\"")
	Integer order;
	
	@Column(name = "\"HaccpItem_id\"")	
	Integer haccpItemId;
	
	@Column(name = "\"HaccpProcess_id\"")
	Integer haccpProcessId;
}
