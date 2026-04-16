package mes.domain.entity.approval;

import lombok.*;

import javax.persistence.*;

@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
@Data
@Table(name = "TB_AA010ATCH")
@Entity
public class TB_AA010ATCH {
  @Id
  @Column(name = "spdate")
  private String spdate;

  @Column(name = "filename")
  private String filename;

  @Lob
  @Column(name = "pdf_data")
  private byte[] pdfData;

  @Column(name = "filepath")
  private String filepath;

  @Column(name = "flag")
  private String flag;

}
