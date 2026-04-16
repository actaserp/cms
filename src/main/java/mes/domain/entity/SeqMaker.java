package mes.domain.entity;

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
@Table(name="seq_maker")
@Setter
@Getter
@NoArgsConstructor
public class SeqMaker {
	
	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	int id;
	
	@Column(name = "\"Code\"")
	String code;
	
	@Column(name = "\"Code2\"")
	String code2;
	
	@Column(name = "\"BaseDate\"")
	String baseDate;
	
	@Column(name = "\"CurrVal\"")
	Integer currVal;
	
	@Column(name = "\"_modified\"")
	Timestamp _modified;
}
