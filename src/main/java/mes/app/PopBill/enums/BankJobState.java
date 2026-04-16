package mes.app.PopBill.enums;

import lombok.Getter;

@Getter
public enum BankJobState {

    RECEIPT("0","접수"),
    WAIT("1", "대기"),
    PROGRESS("2", "진행"),
    COMPLETE("3", "완료"),
    TIMEOUT("4", "시간초과"),
    ERROR("5", "에러발생");

    private final String Code;
    private final String desc;

    BankJobState(String code, String desc){
        this.Code = code;
        this.desc = desc;
    }

    public static BankJobState fromCode(String code){
        for(BankJobState state : BankJobState.values()){
            if(state.Code.equals(code)){
                return state;
            }
        }
        return null;
    }
}
