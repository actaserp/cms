package mes.sse.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class SsePayload {

    private String type;   // NOTIFICATION / COMMENT
    private Object data;   // Notification DTO or Comment DTO
}