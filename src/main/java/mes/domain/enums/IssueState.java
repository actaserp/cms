package mes.domain.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Arrays;

@Getter
@AllArgsConstructor
public enum IssueState {
    SAVE("저장", 100),
    BEFORE_SEND("전송전", 301),
    ISSUED("발행완료", 300),
    WAITING_TO_SEND("전송대기", 302),
    SENDING("전송중", 303),
    SEND_SUCCESS("전송성공", 304),
    SEND_FAILED("전송실패", 305),
    CANCELLED("발행취소", 600);

    private final String label;
    private final int code;

    public static String getLabel(int code){

        //values는 Enum 클래스에서 자동으로 생성되는 모든 enum 상수를 배열로 변환하는 메서드
        return Arrays.stream(values())
                .filter(e -> e.code == code)
                .map(IssueState::getLabel)
                .findFirst().orElse("알 수 없음");

    }
}
