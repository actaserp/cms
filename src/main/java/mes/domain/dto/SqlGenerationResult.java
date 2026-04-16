package mes.domain.dto;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class SqlGenerationResult {
    public String sql;
    public String answer;
    private boolean sqlMode;
    private String content;

    public SqlGenerationResult(boolean sqlMode, String content) {
        this.sqlMode = sqlMode;
        this.content = content;
    }

    public boolean isSqlMode() {
        return sqlMode;
    }

    public String getContent() {
        return content;
    }
}

