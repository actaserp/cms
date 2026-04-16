package mes.app.notification;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Map;

@Getter
@AllArgsConstructor
public class BizEvent {
    private String domain;
    private String action;
    private Object targetId;
    private String spjangcd;
    private String username;
}
