package mes.domain.entity;

import java.sql.Timestamp;
import java.util.Date;

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
@Table(name="board")
@NoArgsConstructor
@Data
@EqualsAndHashCode(callSuper=false)
public class Board extends AbstractAuditModel {
	
	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	int id;
	
	@Column(name = "\"BoardGroup\"")
	String boardGroup;
	
	@Column(name = "\"WriteDateTime\"")
	Timestamp writeDateTime;
	
	@Column(name = "\"Title\"")
	String title;
	
	@Column(name = "\"Content\"")
	String content;
	
	@Column(name = "\"NoticeYN\"")
	String noticeYN;
	
	@Column(name = "\"NoticeEndDate\"")
	Date noticeEndDate;
}
