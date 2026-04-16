package mes.domain.entity.approval;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

import javax.persistence.Column;
import javax.persistence.Embeddable;
import java.io.Serializable;

@Embeddable
@Getter
@Setter
@EqualsAndHashCode
public class TB_E063_PK implements Serializable {

    @Column(length = 2, nullable = false)  // DB에 정의된 spjangcd의 길이는 2
    private String spjangcd;

    @Column(length = 3, nullable = false)  // DB에 정의된 papercd의 길이는 3
    private String papercd;

    @Column(nullable = false) // personid는 int형이므로 length가 필요 없음
    private Integer personid;
}
