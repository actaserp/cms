package mes.domain.entity;

import java.sql.Time;
import java.util.Date;

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
@Table(name="work_calendar")
@Setter
@Getter
@NoArgsConstructor
public class WorkCalendar extends AbstractAuditModel{

	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	int id;
	
	@Column(name = "\"DataDate\"")
	Date DataDate;
	
	@Column(name = "\"StartTime\"")
	Time StartTime;
	
	@Column(name = "\"EndTime\"")
	Time EndTime;
	
	@Column(name = "\"WorkHr\"")
	Float WorkHr;
	
	@Column(name = "\"HolidayYN\"")
	String HolidayYN;
	
	@Column(name = "\"DataPk\"")
	Integer DataPk;
	
	@Column(name = "\"TableName\"")
	String TableName;
}
