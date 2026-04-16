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
@Table(name="routing_proc")
@Setter
@Getter
@NoArgsConstructor
public class RoutingProc extends AbstractAuditModel{

	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	int id;
	
	@Column(name = "\"ProcessOrder\"")
	Float processOrder;
	
	@Column(name = "\"StandardTime\"")
	Float standardTime;
	
	@Column(name = "\"Description\"")
	String description;
	
	@Column(name = "\"Process_id\"")
	Float processId;
	
	@Column(name = "\"Routing_id\"")
	Integer routingId;
}
