package mes.app.traffic.util;

import lombok.AllArgsConstructor;
import org.springframework.web.multipart.MultipartFile;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class NginxLogParser {

    private NginxLogParser() {}

    private static final Pattern LOG_PATTERN = Pattern.compile(
            "\\S+ - - \\[[^]]+] \"\\S+ (\\S+) \\S+\" (\\d+) (\\d+)"
    );

    // 기존 유지
    public static TrafficResult parseFile(File file) {
        try {
            return parseStream(new FileInputStream(file));
        } catch (FileNotFoundException e) {
            throw new RuntimeException("로그 파일 읽기 실패: " + file.getPath(), e);
        }
    }

    // 기존 유지 (수동 업로드용)
    public static TrafficResult parseStream(MultipartFile file) {
        try {
            return parseStream(file.getInputStream());
        } catch (IOException e) {
            throw new RuntimeException("로그 스트림 읽기 실패: " + file.getOriginalFilename(), e);
        }
    }

    // 신규 추가 - 스케줄러(gz 포함)용
    public static TrafficResult parseStream(InputStream inputStream) {
        long totalBytes = 0L;
        long requestCount = 0L;
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                Long bytes = parseLine(line);
                if (bytes != null) { totalBytes += bytes; requestCount++; }
            }
        } catch (IOException e) {
            throw new RuntimeException("로그 스트림 읽기 실패", e);
        }
        return new TrafficResult(totalBytes, requestCount);
    }

    private static Long parseLine(String line) {
        Matcher matcher = LOG_PATTERN.matcher(line);
        if (!matcher.find()) return null;

        String path   = matcher.group(1);
        int    status = Integer.parseInt(matcher.group(2));
        long   bytes  = Long.parseLong(matcher.group(3));

        if (path.contains("/health"))      return null;
        if (status < 200 || status >= 300) return null;

        return bytes;
    }

    @AllArgsConstructor
    public static class TrafficResult {
        public final long totalBytes;
        public final long requestCount;
    }
}