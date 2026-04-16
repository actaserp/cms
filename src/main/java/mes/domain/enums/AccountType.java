package mes.domain.enums;

import lombok.Getter;

import java.util.Arrays;
import java.util.Optional;

@Getter
public enum AccountType {

    COPR("법인", "0"),
    PERSON("개인", "1"),
    ;

    private final String code;
    private final String value;

    AccountType(String code, String value) {
        this.code = code;
        this.value = value;
    }

    // 코드값으로 enum 찾기
    public static Optional<AccountType> fromCode(String code) {
        return Arrays.stream(values())
                .filter(type -> type.code.equals(code))
                .findFirst();
    }

    // 코드값으로 라벨 찾기 (null-safe)
    public static String getLabelByCode(String code) {
        return fromCode(code)
                .map(AccountType::getValue)
                .orElse("알 수 없음");
    }
}
