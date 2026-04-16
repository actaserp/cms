package mes.domain.entity;

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
@Table(name="doc_result")
@NoArgsConstructor
@Data
@EqualsAndHashCode(callSuper=false)
public class DocResult extends AbstractAuditModel {

	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	int id;

	@Column(name = "\"DocumentDate\"")
	Date documentDate;

	@Column(name = "\"DocumentName\"")
	String documentName;

	@Column(name = "\"Content\"")
	String content;

	@Column(name = "\"Description\"")
	String description;

	@Column(name = "\"Number1\"")
	Float number1;

	@Column(name = "\"Number2\"")
	Float number2;

	@Column(name = "\"Number3\"")
	Float number3;

	@Column(name = "\"Text1\"")
	String text1;

	@Column(name = "\"Text2\"")
	String text2;

	@Column(name = "\"Text3\"")
	String text3;

	@Column(name = "\"DocumentForm_id\"")
	Integer documentFormId;
}
