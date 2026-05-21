package mes.app.cms.service;

import lombok.extern.slf4j.Slf4j;
import mes.app.files.NcpObjectStorageService;
import mes.domain.services.SqlRunner;
import org.apache.commons.codec.binary.Base64;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import java.io.ByteArrayOutputStream;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@Service
public class CmsSignService {

    @Autowired
    private SqlRunner sqlRunner;

    // AES 키 (application.properties에 설정)
    // cms.sign.aes-key=16자리 문자열
    @Value("${cms.sign.aes-key}")
    private String aesKey;

    @Value("${app.base-url}")
    private String baseUrl;

    @Autowired
    private NcpObjectStorageService ncpObjectStorageService;

    @PostConstruct
    public void init() {
        log.info("[CmsSignService] aesKey={}, length={}", aesKey, aesKey.length());
    }

    // 토큰 유효기간 (시간)
    private static final int TOKEN_EXPIRE_HOURS = 72;

    /**
     * URL 생성
     * 토큰 = AES 암호화(memberId:spjangcd:expiredAt)
     */
    public String generateSignUrl(Long memberId, String spjangcd) throws Exception {
        // 납부자 존재 확인
        Map<String, Object> member = getMemberById(memberId, spjangcd);
        if (member == null) throw new IllegalStateException("납부자를 찾을 수 없습니다.");

        // 이미 완료된 경우
        if ("Y".equals(member.get("agree_yn"))) {
            throw new IllegalStateException("이미 동의가 완료된 납부자입니다.");
        }

        String plain = memberId + ":" + spjangcd;

        String token = encrypt(plain);
        return baseUrl + "/agree/" + token;
    }

    /**
     * 토큰 검증 및 납부자 데이터 반환
     */
    public Map<String, Object> getSignData(String token) throws Exception {
        String plain = decrypt(token);
        String[] parts = plain.split(":");
        if (parts.length != 2) throw new IllegalStateException("유효하지 않은 링크입니다.");

        Long memberId = Long.parseLong(parts[0]);
        String spjangcd = parts[1];

        Map<String, Object> member = getMemberById(memberId, spjangcd);
        if (member == null) throw new IllegalStateException("납부자 정보를 찾을 수 없습니다.");

        Map<String, Object> result = new HashMap<>();

        // 이미 완료된 경우
        if ("Y".equals(member.get("agree_yn"))) {
            result.put("completed", true);
            return result;
        }

        Map<String, Object> register = sqlRunner.getRow(/* skip_tenant_check */
                "SELECT agree_file_path FROM cms_account_register WHERE member_id = :memberId AND spjangcd = :spjangcd ORDER BY _created DESC LIMIT 1",
                new MapSqlParameterSource("memberId", memberId).addValue("spjangcd", spjangcd));

        if (register != null && register.get("agree_file_path") != null) {
            result.put("completed", true);
            return result;
        }

        // 기관명 조회
        Map<String, Object> workplace = getWorkplace(spjangcd);

        result.put("completed", false);
        result.put("member_id", memberId);
        result.put("spjangcd", spjangcd);
        result.put("company_name", workplace != null ? workplace.get("spjangnm") : "");
        result.put("account_holder", member.get("account_holder") != null ? member.get("account_holder") : member.get("member_name"));
        result.put("id_number", member.get("id_number"));
        result.put("bank_name", member.get("bank_name"));
        result.put("bank_account", member.get("bank_account"));
        result.put("phone", member.get("phone"));
        result.put("member_name", member.get("member_name"));
        result.put("charge_type", workplace != null ? workplace.get("cms_description") : "CMS 자동이체");
        result.put("token", token);


        return result;
    }

    /**
     * 서명 제출 처리
     * 1. 토큰 재검증
     * 2. cms_member 업데이트 (계좌정보)
     * 3. PDF 생성
     */
    public void submitSign(Map<String, String> payload) throws Exception {
        String token = payload.get("token");
        if (token == null) throw new IllegalStateException("토큰이 없습니다.");

        // 토큰 재검증
        String plain = decrypt(token);
        String[] parts = plain.split(":");
        Long memberId = Long.parseLong(parts[0]);
        String spjangcd = parts[1];

        Map<String, Object> member = getMemberById(memberId, spjangcd);
        if (member == null) throw new IllegalStateException("납부자를 찾을 수 없습니다.");
        if ("Y".equals(member.get("agree_yn"))) {
            throw new IllegalStateException("이미 완료된 신청입니다.");
        }

        Map<String, Object> existingRegister = sqlRunner.getRow(/* skip_tenant_check */
                "SELECT agree_file_path FROM cms_account_register WHERE member_id = :memberId AND spjangcd = :spjangcd ORDER BY _created DESC LIMIT 1",
                new MapSqlParameterSource("memberId", memberId).addValue("spjangcd", spjangcd));
        if (existingRegister != null && existingRegister.get("agree_file_path") != null) {
            throw new IllegalStateException("이미 동의서가 제출되었습니다.");
        }

        String accountHolder = payload.get("account_holder");
        String idNumber = payload.get("id_number");
        String bankName = payload.get("bank_name");
        String bankAccount = payload.get("bank_account");
        String phone = payload.get("phone");
        String signImage = payload.get("sign_image"); // base64 data URL

        // 은행코드 조회 (bank_name → bank_code)
        String bankCode = getBankCodeByName(bankName);

        // 1. cms_member 업데이트
        updateMember(memberId, spjangcd, accountHolder, idNumber, bankCode, bankAccount, phone);

        // 2. PDF 생성
        Map<String, Object> workplace = getWorkplace(spjangcd);
        String companyName = workplace != null ? String.valueOf(workplace.get("spjangnm")) : "";
        String memberName = String.valueOf(member.get("member_name"));
        String chargeType = workplace != null && workplace.get("cms_description") != null
                ? String.valueOf(workplace.get("cms_description")) : "CMS 자동이체";
        byte[] pdfBytes = generatePdf(companyName, accountHolder, idNumber, bankName, bankAccount, phone, signImage, memberName, chargeType);

        Map<String, Object> register = sqlRunner.getRow(/* skip_tenant_check */
                "SELECT id FROM cms_account_register WHERE member_id = :memberId AND spjangcd = :spjangcd ORDER BY _created DESC LIMIT 1",
                new MapSqlParameterSource("memberId", memberId)
                        .addValue("spjangcd", spjangcd));
        Long registerId = register != null ? ((Number) register.get("id")).longValue() : memberId;


        // 3. 파일 저장
        String objectKey = saveAgreeFile(registerId, spjangcd, pdfBytes, String.valueOf(member.get("member_name")));

        sqlRunner.execute(/* skip_tenant_check */
                "UPDATE cms_account_register SET agree_file_path = :filePath, agree_ext = 'pdf', agree_type = '3' WHERE id = :id AND spjangcd = :spjangcd",
                new MapSqlParameterSource()
                        .addValue("filePath", objectKey)
                        .addValue("id", registerId)
                        .addValue("spjangcd", spjangcd));
        log.info("[CmsSign] 서명 완료 - memberId: {}, spjangcd: {}", memberId, spjangcd);
    }

    /**
     * PDF 생성 (iText 7 기반)
     */
    private byte[] generatePdf(String companyName, String accountHolder, String idNumber,
                               String bankName, String bankAccount, String phone,
                               String signImageBase64, String memberName, String chargeType) throws Exception {

        String html = """
        <!DOCTYPE html>
        <html>
        <head>
        <meta charset="UTF-8"/>
        <style>
            body { font-family: 'Noto Sans KR', serif; font-size: 11px; margin: 20px 30px; }
            h2 { text-align: center; font-size: 15px; margin-bottom: 16px; }
            .section { font-weight: bold; border-bottom: 2px solid #000; margin: 12px 0 5px; font-size: 11px; }
            table { width: 100%%; border-collapse: collapse; margin-bottom: 8px; }
            th { background: #f5f5f5; border: 1px solid #999; padding: 5px 8px;
                 text-align: left; width: 25%%; font-size: 10px; }
            td { border: 1px solid #999; padding: 5px 8px; font-size: 10px; }
            .agree-box { border: 1px solid #999; padding: 8px; margin-bottom: 8px;
                         font-size: 9px; line-height: 1.6; }
            .agree-title { font-weight: bold; margin-bottom: 4px; }
            .agree-row { text-align: right; margin-top: 4px; }
            .right { text-align: right; margin: 10px 0; font-size: 11px; }
            .sign-img { margin-top: 4px; }
            .notice { font-size: 9px; color: #555; margin-top: 8px; }
            .notice li { margin-left: 14px; line-height: 1.7; }
        </style>
        </head>
        <body>
        <h2>(지로/CMS/펌뱅킹) 자동이체 신청서</h2>

        <div class="section">▢ 수납기관 및 요금 종류</div>
        <table>
            <tr><th>수납기관명</th><td>%s</td></tr>
            <tr><th>수납 요금종류</th><td>%s</td></tr>
        </table>

        <div class="section">▢ 출금이체 신청 내용 (신청고객 기재란)</div>
        <table>
            <tr><th>계좌주명</th><td>%s</td>
                <th>생년월일(사업자등록번호)</th><td>%s</td></tr>
            <tr><th>금융기관명</th><td>%s</td>
                <th>출금계좌번호</th><td>%s</td></tr>
            <tr><th>계좌주 연락처</th><td colspan="3">%s</td></tr>
        </table>

        <div class="agree-box">
            <div class="agree-title">[개인(신용)정보 수집 및 이용 동의]</div>
            수집항목: [필수] 금융기관명, 출금계좌번호, 성명, 생년월일, 연락처(휴대폰번호 등)<br/>
            수집목적: 자동이체 서비스의 제공 | 보유기간: 자동이체 이용 종료 또는 해지 후 5년 까지<br/>
            신청인은 위의 개인(신용)정보 수집 및 이용을 거부할 권리가 있으나, 권리행사 시 자동이체 신청이 거부될 수 있습니다.
            <div class="agree-row"><span></span><span>동의함 [V] &#160;&#160; 동의안함 □</span></div>
        </div>

        <div class="agree-box">
            <div class="agree-title">[개인(신용)정보 제3자 제공 동의]</div>
            제공받는 자: 사단법인 금융결제원, 상기 청구기관(이용기관)<br/>
            제공항목: [필수] 금융기관명, 출금계좌번호, 성명, 생년월일, 연락처(휴대폰번호 등)<br/>
            제공목적: 자동이체 서비스의 제공, 자동이체 출금동의 확인, 자동이체 신규 등록 및 해지 사실 통지<br/>
            보유기간: 자동이체 이용 종료 또는 해지 후 5년 까지<br/>
            신청인은 위의 개인(신용)정보 제3자 제공을 거부할 권리가 있으나, 권리행사 시 자동이체 신청이 거부될 수 있습니다.
            <div class="agree-row"><span></span><span>동의함 [V] &#160;&#160; 동의안함 □</span></div>
        </div>

        <p style="font-size:9px; margin-bottom:8px;">
            [출금이체 동의여부 및 해지사실 통지 안내] 은행 등 금융회사 및 금융결제원은 자동이체 제도의 안정적 운영을 위하여
            고객의 연락처 정보를 활용하여 문자메세지, 유선 등으로 고객의 출금이체 동의여부 및 해지사실을 통지할 수 있으니
            올바른 연락처 등록여부를 확인하시기 바랍니다.
        </p>

        <p style="font-size:10px; text-align:center; margin-bottom:10px;">
            상기 금융거래정보의 제공 및 개인(신용)정보의 수집 및 이용, 제3자 제공에 동의하며 자동이체를 신청합니다.
        </p>

        <p class="right">%s</p>

                <table style="margin-top:8px; width:100%%;">
                    <tr>
                        <th style="width:15%%; text-align:center;">신청인</th>
                        <td style="width:25%%;">%s</td>
                        <th style="width:15%%; text-align:center;">서명</th>
                        <td style="width:45%%;"><img src="%s" style="max-width:200px; height:60px;"/></td>
                    </tr>
                </table>

        <ul class="notice">
            <li>인감 또는 서명은 해당 예금계좌 사용인감 또는 서명을 날인하여야 합니다.</li>
            <li>기존 신청내용을 변경하고자 하는 경우에는 먼저 해지신청을 하고 신규 작성을 하여야 합니다.</li>
            <li>주계약자와 예금주가 다른 경우 반드시 예금주의 별도 서명을 받아야 합니다.</li>
        </ul>
        </body>
        </html>
        """.formatted(
                companyName,
                chargeType,
                accountHolder, idNumber, bankName, bankAccount, phone,
                java.time.LocalDate.now().format(
                        java.time.format.DateTimeFormatter.ofPattern("yyyy년 MM월 dd일")),
                memberName,
                signImageBase64
        );

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        com.openhtmltopdf.pdfboxout.PdfRendererBuilder builder =
                new com.openhtmltopdf.pdfboxout.PdfRendererBuilder();
        java.net.URL fontUrl = getClass().getResource("/static/font/NotoSansKR-Regular.ttf");
        if (fontUrl != null) {
            builder.useFont(new java.io.File(fontUrl.toURI()), "Noto Sans KR");
        }
        builder.useFastMode();
        builder.withHtmlContent(html, null);
        builder.toStream(baos);
        builder.run();

        return baos.toByteArray();
    }

    /**
     * 동의서 파일 저장
     */
    private String saveAgreeFile(Long memberId, String spjangcd, byte[] pdfBytes, String member_name) throws Exception {
        String uuid = java.util.UUID.randomUUID().toString();
        String fileName = "agree_" + memberId + "_" + uuid + ".pdf";
        String objectKey = ncpObjectStorageService.buildObjectKey(spjangcd, "cms_member", fileName);
        String filePrefix = ncpObjectStorageService.getFilePrefix(spjangcd, "cms_member");

        // NCP 업로드
        ncpObjectStorageService.upload(
                objectKey,
                new java.io.ByteArrayInputStream(pdfBytes),
                pdfBytes.length,
                "application/pdf"
        );

        // 기존 동의서 파일 삭제
        Map<String, Object> existing = sqlRunner.getRow(/* skip_tenant_check */
                """
                SELECT f.fileseq, f.filepath, f.filesvnm
                FROM TB_FILEINFO f
                WHERE f.bbsseq = :memberId
                  AND f.checkseq = :checkseq
                  AND f.spjangcd = :spjangcd
                """,
                new MapSqlParameterSource()
                        .addValue("memberId", memberId.intValue())
                        .addValue("checkseq", NcpObjectStorageService.toCheckseq("AGREE"))
                        .addValue("spjangcd", spjangcd));

        if (existing != null) {
            // NCP 삭제
            String oldKey = existing.get("filepath") + "/" + existing.get("filesvnm");
            try { ncpObjectStorageService.delete(oldKey); } catch (Exception e) {
                log.warn("[CmsSign] 기존 파일 NCP 삭제 실패: {}", oldKey);
            }
            // DB 삭제
            sqlRunner.execute(/* skip_tenant_check */
                    "DELETE FROM TB_FILEINFO WHERE fileseq = :fileseq",
                    new MapSqlParameterSource("fileseq", existing.get("fileseq")));
        }

        // DB 저장
        String today = java.time.LocalDate.now().format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd"));
        sqlRunner.execute(/* skip_tenant_check */
                """
                INSERT INTO TB_FILEINFO
                    (FILEDATE, CHECKSEQ, bbsseq, FILEPATH, FILESVNM, FILEEXTNS, FILEORNM, FILESIZE, INDATEM, INUSERID, spjangcd)
                VALUES
                    (:filedate, :checkseq, :bbsseq, :filepath, :filesvnm, 'pdf', :fileornm, :filesize, NOW(), 'SYSTEM', :spjangcd)
                """,
                new MapSqlParameterSource()
                        .addValue("filedate", today)
                        .addValue("checkseq", NcpObjectStorageService.toCheckseq("AGREE"))
                        .addValue("bbsseq", memberId.intValue())
                        .addValue("filepath", filePrefix)
                        .addValue("filesvnm", fileName)
                        .addValue("fileornm", "자동이체동의서_" + member_name + ".pdf")
                        .addValue("filesize", pdfBytes.length)
                        .addValue("spjangcd", spjangcd));

        log.info("[CmsSign] 동의서 저장 완료 - memberId: {}, objectKey: {}", memberId, objectKey);
        return objectKey;
    }

    private void updateMember(Long memberId, String spjangcd, String accountHolder,
                              String idNumber, String bankCode, String bankAccount, String phone) {
        var param = new MapSqlParameterSource();
        param.addValue("id", memberId);
        param.addValue("spjangcd", spjangcd);
        param.addValue("accountHolder", accountHolder);
        param.addValue("idNumber", idNumber);
        param.addValue("bankCode", bankCode);
        param.addValue("bankAccount", bankAccount);
        param.addValue("phone", phone);

        sqlRunner.execute(/* skip_tenant_check */
                """
                UPDATE cms_member SET
                    account_holder = :accountHolder,
                    id_number      = :idNumber,
                    bank_code      = COALESCE(:bankCode, bank_code),
                    bank_account   = :bankAccount,
                    phone          = :phone,
                    _modified      = NOW()
                WHERE id = :id AND spjangcd = :spjangcd
                """, param);
    }

    private Map<String, Object> getMemberById(Long memberId, String spjangcd) {
        return sqlRunner.getRow(/* skip_tenant_check */
                """
                SELECT m.*, b.bank_name
                FROM cms_member m
                LEFT JOIN cms_bank_code b ON b.bank_code = m.bank_code
                WHERE m.id = :id AND m.spjangcd = :spjangcd
                """,
                new MapSqlParameterSource("id", memberId).addValue("spjangcd", spjangcd));
    }

    private Map<String, Object> getWorkplace(String spjangcd) {
        return sqlRunner.getRow(/* skip_tenant_check */
                """
                    SELECT w.spjangnm, c.cms_description
                    FROM tb_xa012 w
                    LEFT JOIN tb_xa012_cms c ON c.spjangcd = w.spjangcd
                    WHERE w.spjangcd = :spjangcd
                """,
                new MapSqlParameterSource("spjangcd", spjangcd));
    }

    private String getBankCodeByName(String bankName) {
        if (bankName == null || bankName.trim().isEmpty()) return null;
        Map<String, Object> row = sqlRunner.getRow(/* skip_tenant_check */
                "SELECT bank_code FROM cms_bank_code WHERE bank_name = :bankName",
                new MapSqlParameterSource("bankName", bankName.trim()));
        return row != null ? (String) row.get("bank_code") : null;
    }

    // ─── 양식 다운로드 ───────────────────────────────────

    private static final String FORM_OBJECT_KEY = "cms/common/자동이체신청서.pdf";

    public void streamFormFile(javax.servlet.http.HttpServletResponse response) throws Exception {
        software.amazon.awssdk.core.ResponseInputStream<software.amazon.awssdk.services.s3.model.GetObjectResponse> s3Stream =
                ncpObjectStorageService.download(FORM_OBJECT_KEY);

        response.setContentType("application/pdf");
        response.setHeader("Content-Disposition",
                "attachment; filename*=UTF-8''" + java.net.URLEncoder.encode("자동이체신청서.pdf", "UTF-8"));

        byte[] buffer = new byte[8192];
        int bytesRead;
        try (java.io.BufferedOutputStream out = new java.io.BufferedOutputStream(response.getOutputStream())) {
            while ((bytesRead = s3Stream.read(buffer)) != -1) {
                out.write(buffer, 0, bytesRead);
            }
            out.flush();
        } finally {
            s3Stream.close();
        }
    }

    // ─── 파일 첨부 제출 ──────────────────────────────────

    public void submitFile(String token,
                           org.springframework.web.multipart.MultipartFile file,
                           String accountHolder, String idNumber,
                           String bankName, String bankAccount, String phone) throws Exception {

        // 토큰 검증
        String plain = decrypt(token);
        String[] parts = plain.split(":");
        Long memberId = Long.parseLong(parts[0]);
        String spjangcd = parts[1];

        Map<String, Object> member = getMemberById(memberId, spjangcd);
        if (member == null) throw new IllegalStateException("납부자를 찾을 수 없습니다.");
        if ("Y".equals(member.get("agree_yn"))) throw new IllegalStateException("이미 완료된 신청입니다.");

        // 파일 확장자 확인
        String originalFilename = file.getOriginalFilename();
        if (originalFilename == null) throw new IllegalStateException("파일명이 없습니다.");
        String ext = originalFilename.substring(originalFilename.lastIndexOf(".") + 1).toLowerCase();
        java.util.List<String> allowed = java.util.Arrays.asList("pdf", "jpg", "jpeg", "png", "gif", "tif");
        if (!allowed.contains(ext)) throw new IllegalStateException("허용되지 않는 파일 형식입니다. (pdf, jpg, jpeg, png, gif, tif)");
        if (file.getSize() > 300 * 1024) {
            throw new IllegalStateException("동의자료 파일은 300KB 이하만 업로드 가능합니다.");
        }
        // 은행코드 조회
        String bankCode = getBankCodeByName(bankName);

        // 1. cms_member 업데이트
        updateMember(memberId, spjangcd, accountHolder, idNumber, bankCode, bankAccount, phone);

        // 2. register 조회
        Map<String, Object> register = sqlRunner.getRow(/* skip_tenant_check */
                "SELECT id FROM cms_account_register WHERE member_id = :memberId AND spjangcd = :spjangcd ORDER BY _created DESC LIMIT 1",
                new MapSqlParameterSource("memberId", memberId).addValue("spjangcd", spjangcd));
        Long registerId = register != null ? ((Number) register.get("id")).longValue() : memberId;

        // 3. 파일 저장 (NCP + TB_FILEINFO)
        String memberName = String.valueOf(member.get("member_name"));
        String uuid = java.util.UUID.randomUUID().toString();
        String fileName = "agree_" + registerId + "_" + uuid + "." + ext;
        String objectKey = ncpObjectStorageService.buildObjectKey(spjangcd, "cms_member", fileName);
        String filePrefix = ncpObjectStorageService.getFilePrefix(spjangcd, "cms_member");

        // 기존 파일 삭제
        Map<String, Object> existing = sqlRunner.getRow(/* skip_tenant_check */
                "SELECT fileseq, filepath, filesvnm FROM TB_FILEINFO WHERE bbsseq = :bbsseq AND checkseq = :checkseq AND spjangcd = :spjangcd",
                new MapSqlParameterSource()
                        .addValue("bbsseq", registerId.intValue())
                        .addValue("checkseq", NcpObjectStorageService.toCheckseq("AGREE"))
                        .addValue("spjangcd", spjangcd));
        if (existing != null) {
            try { ncpObjectStorageService.delete(existing.get("filepath") + "/" + existing.get("filesvnm")); }
            catch (Exception e) { log.warn("[CmsSign] 기존 파일 NCP 삭제 실패"); }
            sqlRunner.execute(/* skip_tenant_check */
                    "DELETE FROM TB_FILEINFO WHERE fileseq = :fileseq",
                    new MapSqlParameterSource("fileseq", existing.get("fileseq")));
        }

        // NCP 업로드
        ncpObjectStorageService.upload(objectKey, file.getInputStream(), file.getSize(),
                file.getContentType() != null ? file.getContentType() : "application/octet-stream");

        // DB 저장
        String today = java.time.LocalDate.now().format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd"));
        sqlRunner.execute(/* skip_tenant_check */
                """
                INSERT INTO TB_FILEINFO
                    (FILEDATE, CHECKSEQ, bbsseq, FILEPATH, FILESVNM, FILEEXTNS, FILEORNM, FILESIZE, INDATEM, INUSERID, spjangcd)
                VALUES
                    (:filedate, :checkseq, :bbsseq, :filepath, :filesvnm, :ext, :fileornm, :filesize, NOW(), 'SYSTEM', :spjangcd)
                """,
                new MapSqlParameterSource()
                        .addValue("filedate", today)
                        .addValue("checkseq", NcpObjectStorageService.toCheckseq("AGREE"))
                        .addValue("bbsseq", registerId.intValue())
                        .addValue("filepath", filePrefix)
                        .addValue("filesvnm", fileName)
                        .addValue("ext", ext)
                        .addValue("fileornm", "자동이체동의서_" + memberName + "." + ext)
                        .addValue("filesize", (int) file.getSize())
                        .addValue("spjangcd", spjangcd));

        // 4. agree_file_path 업데이트
        sqlRunner.execute(/* skip_tenant_check */
                "UPDATE cms_account_register SET agree_file_path = :filePath, agree_ext = :ext WHERE id = :id AND spjangcd = :spjangcd",
                new MapSqlParameterSource()
                        .addValue("filePath", objectKey)
                        .addValue("ext", ext)
                        .addValue("id", registerId)
                        .addValue("spjangcd", spjangcd));

        log.info("[CmsSign] 파일 첨부 완료 - memberId: {}, registerId: {}, file: {}", memberId, registerId, fileName);
    }

    // ─── AES 암호화/복호화 ───────────────────────────────

    private String encrypt(String plain) throws Exception {
        SecretKeySpec key = new SecretKeySpec(aesKey.getBytes("UTF-8"), "AES");
        Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
        cipher.init(Cipher.ENCRYPT_MODE, key);
        byte[] encrypted = cipher.doFinal(plain.getBytes("UTF-8"));
        // URL-safe base64
        return Base64.encodeBase64URLSafeString(encrypted);
    }

    private String decrypt(String token) throws Exception {
        SecretKeySpec key = new SecretKeySpec(aesKey.getBytes("UTF-8"), "AES");
        Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
        cipher.init(Cipher.DECRYPT_MODE, key);
        byte[] decoded = Base64.decodeBase64(token);
        return new String(cipher.doFinal(decoded), "UTF-8");
    }
}