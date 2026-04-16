package mes.domain.entity;

import java.sql.Timestamp;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

import lombok.AllArgsConstructor;
import lombok.Data;

@Entity
@Table(name="sys_log")
@AllArgsConstructor
@Data
public class SystemLog {
	
	public SystemLog() {
		this._created = new Timestamp(System.currentTimeMillis());
	}
	
	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	long id;	
	
	@Column(name = "\"Type\"")
	String type;
	
	@Column(name = "\"Source\"")	
	String source;
	
	
	@Column(name = "\"Message\"", columnDefinition="TEXT")
	String message;
	
	@Column(name = "_created")
	Timestamp _created;

	@Column(name = "spjangcd")
	String spjangcd;

	
}
