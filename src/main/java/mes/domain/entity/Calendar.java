package mes.domain.entity;

import java.sql.Time;
import java.sql.Timestamp;

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
@Table(name="calendar")
@Setter
@Getter
@NoArgsConstructor
public class Calendar extends AbstractAuditModel {

	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	int id;
	
	@Column(name = "\"DataDate\"")
	Timestamp dataDate;
	
	@Column(name = "\"StartTime\"")
	Time startTime;
	
	@Column(name = "\"EndTime\"")
	Time endTime;
	
	@Column(name = "\"Title\"")
	String title;
	
	@Column(name = "\"Color\"")
	String color;
	
	@Column(name = "\"Description\"")
	String description;
}
