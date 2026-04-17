package mes.app.files;

import lombok.extern.slf4j.Slf4j;
import mes.config.Settings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import javax.annotation.PostConstruct;
import java.io.InputStream;
import java.net.URI;
import java.util.Map;

@Slf4j
@Service
public class NcpObjectStorageService {

    @Autowired
    private Settings settings;

    private S3Client s3Client;
    private String bucketName;

    @Value("${mes.project-name}")
    private String projectName;

    @PostConstruct
    public void init() {
        String endpoint  = settings.getProperty("ncp.storage.endpoint");
        String region    = settings.getProperty("ncp.storage.region");
        String accessKey = settings.getProperty("ncp_api_accessKey");
        String secretKey = settings.getProperty("ncp_api_secretKey");
        this.bucketName  = settings.getProperty("ncp.storage.bucket");

        this.s3Client = S3Client.builder()
                .endpointOverride(URI.create(endpoint))
                .region(Region.of(region))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(accessKey, secretKey)))
                .build();
    }

    /**
     * NCP 오브젝트 스토리지에 파일 업로드
     * @param objectKey  저장 경로: {spjangcd}/{tableName}/{uuid}.{ext}
     * @param inputStream 파일 스트림
     * @param contentLength 파일 크기 (bytes)
     * @param contentType  MIME 타입
     */
    public void upload(String objectKey, InputStream inputStream, long contentLength, String contentType) {
        PutObjectRequest request = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(objectKey)
                .contentType(contentType)
                .contentLength(contentLength)
                .build();

        s3Client.putObject(request, RequestBody.fromInputStream(inputStream, contentLength));
        log.info("[NcpStorage] 업로드 완료: {}/{}", bucketName, objectKey);
    }

    /**
     * NCP 오브젝트 스토리지에서 파일 다운로드
     * @param objectKey 조회할 오브젝트 키
     * @return ResponseInputStream (S3Object)
     */
    public ResponseInputStream<GetObjectResponse> download(String objectKey) {
        GetObjectRequest request = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(objectKey)
                .build();
        return s3Client.getObject(request);
    }

    /**
     * NCP 오브젝트 스토리지에서 파일 삭제
     * @param objectKey 삭제할 오브젝트 키
     */
    public void delete(String objectKey) {
        DeleteObjectRequest request = DeleteObjectRequest.builder()
                .bucket(bucketName)
                .key(objectKey)
                .build();
        s3Client.deleteObject(request);
        log.info("[NcpStorage] 삭제 완료: {}/{}", bucketName, objectKey);
    }

    // TB_FILEINFO.CHECKSEQ: varchar(2) — 기능명 → 코드값 매핑
    private static final Map<String, String> CHECKSEQ_MAP = Map.of(
        "NOTICE",    "01",
        "QNA",       "02",
        "MARKETING", "03",
        "PDF","04",
        "ATCH","05",
            "AGREE","06"
    );

    public static String toCheckseq(String tableName) {
        return CHECKSEQ_MAP.getOrDefault(tableName != null ? tableName.toUpperCase() : "", "99");
    }

    /**
     * 오브젝트 키 생성: {dbKey}/{featureCode}/{uuid}.{ext}
     * @param spjangcd      사업장 DB 키 (User.dbKey) — 테넌트 DB 단위로 버킷 폴더 구분
     * @param featureCode 기능 식별자 (예: NOTICE, QNA) — TB_FILEINFO.CHECKSEQ 와 동일 값
     * @param uuidFileName 저장 파일명 (uuid + 확장자)
     */
    public String buildObjectKey(String spjangcd, String featureCode, String uuidFileName) {
        return this.projectName + "/" + spjangcd + "/" + featureCode + "/" + uuidFileName;
    }

    public String getFilePrefix(String spjangcd, String featureCode) {
        return this.projectName + "/" + spjangcd + "/" + featureCode;
    }
}
