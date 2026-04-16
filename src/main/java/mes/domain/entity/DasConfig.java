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
@Table(name="das_config")
@NoArgsConstructor
@Data
@EqualsAndHashCode(callSuper=false)
public class DasConfig extends AbstractAuditModel {
	
	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	int id;
	
	@Column(name = "\"Name\"")
	String name;
	
	@Column(name = "\"Description\"")
	String description;
	
	@Column(name = "\"Configuration\"")
	String configuration;
	
	@Column(name = "\"Handler\"")
	String handler;
	
	@Column(name = "\"Topic\"")
	String topic;
	
	@Column(name = "\"DeviceType\"")
	String deviceType;
	
	@Column(name = "\"ConfigFileName\"")
	String configFileName;
	
	@Column(name = "\"is_active\"")
	String is_active;
	
	@Column(name = "\"Equipment_id\"")
	Integer equipment_id;
	
	@Column(name = "\"Server_id\"")
	Integer server_id;
}
