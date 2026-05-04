package mes.app.cms.service;

import lombok.extern.slf4j.Slf4j;
import mes.app.common.TenantContext;
import mes.app.files.NcpObjectStorageService;
import mes.domain.services.SqlRunner;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletResponse;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class CmsMemberService {

    @Autowired
    SqlRunner sqlRunner;

    @Autowired
    private CmsAccountRegisterService cmsAccountRegisterService;

    @Autowired
    private NcpObjectStorageService storageService;

    private String str(Object v) { return v != null ? v.toString() : ""; }

    /** 납부자 목록 조회 */
    public List<Map<String, Object>> getMemberList(String memberName, String memberNo, String status) {
        String spjangcd = TenantContext.get();
        MapSqlParameterSource param = new MapSqlParameterSource();
        param.addValue("spjangcd", spjangcd);

        String sql = """
            SELECT m.id
                 , m.member_type
                 , CASE m.member_type WHEN 'C' THEN '법인' WHEN 'S' THEN '개인사업자' ELSE '개인' END AS member_type_name
                 , m.member_name
                 , m.member_no
                 , m.id_number
                 , m.phone
                 , m.email
                 , m.zipcd
                 , m.adresa
                 , m.adresb
                 , m.bank_code
                 , b.bank_name
                 , m.bank_account
                 , m.account_holder
                 , m.deduct_day
                 , m.deduct_amount
                 , m.cycle_type
                 , m.cycle_months
                 , m.start_date
                 , m.end_date
                 , m.agree_yn
                 , m.agree_date
                 , m.agree_method
                 , m.status
                 , m.memo
                 , m._created
                 , m._modified
                 , CASE
                       WHEN m.agree_yn = 'Y'        THEN '인증완료'
                       WHEN r.status = 'REJECTED'   THEN '인증거절'
                       WHEN r.status IN ('PENDING')  THEN '인증대기'
                       WHEN r.status = 'FAILED'     THEN '인증실패'
                       ELSE '미신청'
                   END AS agree_status
            FROM cms_member m
            LEFT JOIN cms_bank_code b ON b.bank_code = m.bank_code
            LEFT JOIN LATERAL (
                SELECT status FROM cms_account_register
                WHERE member_id = m.id AND spjangcd = m.spjangcd
                ORDER BY _created DESC LIMIT 1
            ) r ON true
            WHERE m.spjangcd = :spjangcd
            """;

        if (StringUtils.hasText(memberName)) {
            sql += " AND m.member_name LIKE '%' || :memberName || '%'";
            param.addValue("memberName", memberName);
        }
        if (StringUtils.hasText(memberNo)) {
            sql += " AND m.member_no LIKE '%' || :memberNo || '%'";
            param.addValue("memberNo", memberNo);
        }
        if (StringUtils.hasText(status)) {
            sql += " AND m.status = :status";
            param.addValue("status", status);
        }

        sql += " ORDER BY m.id DESC";
        return sqlRunner.getRows(sql, param);
    }

    /** 납부자 단건 조회 */
    public Map<String, Object> getMember(Long id) {
        String spjangcd = TenantContext.get();
        MapSqlParameterSource param = new MapSqlParameterSource();
        param.addValue("id", id);
        param.addValue("spjangcd", spjangcd);

        String sql = """
                SELECT m.id
                     , m.member_type
                     , m.member_name
                     , m.member_no
                     , m.id_number
                     , m.phone
                     , m.email
                     , m.zipcd
                     , m.adresa
                     , m.adresb
                     , m.bank_code
                     , b.bank_name
                     , m.bank_account
                     , m.account_holder
                     , m.deduct_day
                     , m.deduct_amount
                     , m.cycle_type
                     , m.cycle_months
                     , m.start_date
                     , m.end_date
                     , m.agree_yn
                     , m.agree_date
                     , m.agree_method
                     , m.status
                     , m.memo
                FROM cms_member m
                LEFT JOIN cms_bank_code b ON b.bank_code = m.bank_code
                WHERE m.id = :id AND m.spjangcd = :spjangcd
                """;
        return sqlRunner.getRow(sql, param);
    }

    /** 납부자 저장 (신규/수정) */
    public Long saveMember(Long id, String memberType, String memberName, String memberNo,
                           String idNumber, String phone, String email,
                           String zipcd, String adresa, String adresb,
                           String bankCode, String bankAccount, String accountHolder,
                           String deductDay, Long deductAmount,
                           String cycleType, String cycleMonths,
                           String startDate, String endDate,
                           String agreeYn, String agreeMethod,
                           String status, String memo,
                           String userId) {

        String spjangcd = TenantContext.get();
        MapSqlParameterSource param = new MapSqlParameterSource();
        param.addValue("spjangcd", spjangcd);
        param.addValue("memberType", memberType != null ? memberType : "P");
        param.addValue("memberName", memberName);
        param.addValue("memberNo", memberNo);
        param.addValue("idNumber", idNumber);
        param.addValue("phone", phone);
        param.addValue("email", email);
        param.addValue("zipcd", zipcd);
        param.addValue("adresa", adresa);
        param.addValue("adresb", adresb);
        param.addValue("bankCode", bankCode);
        param.addValue("bankAccount", bankAccount);
        param.addValue("accountHolder", accountHolder);
        param.addValue("deductDay", deductDay);
        param.addValue("deductAmount", deductAmount);
        param.addValue("cycleType", cycleType != null ? cycleType : "REGULAR");
        // 비정기일 때 cycle_months 는 NULL
        param.addValue("cycleMonths", "IRREGULAR".equals(cycleType) ? null : cycleMonths);
        param.addValue("startDate", startDate);
        param.addValue("endDate", endDate != null ? endDate : "99991231");
        param.addValue("agreeYn", agreeYn != null ? agreeYn : "N");
        param.addValue("agreeMethod", agreeMethod);
        param.addValue("status", status != null ? status : "ACTIVE");
        param.addValue("memo", memo);
        param.addValue("userId", userId);

        if (id == null) {
            // 납부자번호 자동 채번
            Map<String, Object> seqRow = sqlRunner.getRow(
                    """
                    SELECT COALESCE(MAX(CAST(SUBSTRING(member_no, LENGTH(:spjangcd) + 1) AS INTEGER)), 0) + 1 AS next_seq
                    FROM cms_member
                    WHERE spjangcd = :spjangcd
                      AND member_no ~ ('^' || :spjangcd || '[0-9]+$')
                    """,
                    new MapSqlParameterSource("spjangcd", spjangcd));
            int nextSeq = seqRow != null ? ((Number) seqRow.get("next_seq")).intValue() : 1;
            String autoMemberNo = spjangcd + String.format("%06d", nextSeq);
            param.addValue("memberNo", autoMemberNo);

            String sql = """
                    INSERT INTO cms_member (
                        spjangcd, member_type, member_name, member_no, id_number,
                        phone, email, zipcd, adresa, adresb,
                        bank_code, bank_account, account_holder,
                        deduct_day, deduct_amount, cycle_type, cycle_months, start_date, end_date,
                        agree_yn, agree_date, agree_method,
                        status, memo,
                        _creater_id, _created, _modifier_id, _modified
                    ) VALUES (
                        :spjangcd, :memberType, :memberName, :memberNo, :idNumber,
                        :phone, :email, :zipcd, :adresa, :adresb,
                        :bankCode, :bankAccount, :accountHolder,
                        :deductDay, :deductAmount, :cycleType, :cycleMonths, :startDate, :endDate,
                        :agreeYn, CASE WHEN :agreeYn = 'Y' THEN NOW() ELSE NULL END, :agreeMethod,
                        :status, :memo,
                        :userId, NOW(), :userId, NOW()
                    ) RETURNING id
                    """;
            Map<String, Object> row = sqlRunner.getRow(sql, param);
            if (row == null) return null;
            Long savedId = ((Number) row.get("id")).longValue();

            String checkseq = NcpObjectStorageService.toCheckseq("cms_member");
            Map<String, Object> fileInfo = sqlRunner.getRow(
                    """
                    SELECT filepath, filesvnm, fileextns
                    FROM TB_FILEINFO
                    WHERE checkseq = :checkseq
                      AND bbsseq   = :bbsseq
                      AND spjangcd = :spjangcd
                    ORDER BY fileseq DESC
                    LIMIT 1
                    """,
                    new MapSqlParameterSource("checkseq", checkseq)
                            .addValue("bbsseq", savedId.intValue())
                            .addValue("spjangcd", spjangcd));

            String agreeFilePath = null;
            String agreeExt      = null;
            if (fileInfo != null) {
                agreeFilePath = fileInfo.get("filepath") + "/" + fileInfo.get("filesvnm");
                agreeExt      = str(fileInfo.get("fileextns")).toLowerCase();
            }

            // cms_account_register 자동 생성
            cmsAccountRegisterService.save(savedId, "1", agreeExt, agreeFilePath, userId);
            return  savedId;

        } else {
            param.addValue("id", id);
            String sql = """
                    UPDATE cms_member SET
                        member_type    = :memberType,
                        member_name    = :memberName,
                        member_no      = :memberNo,
                        id_number      = :idNumber,
                        phone          = :phone,
                        email          = :email,
                        zipcd          = :zipcd,
                        adresa         = :adresa,
                        adresb         = :adresb,
                        bank_code      = :bankCode,
                        bank_account   = :bankAccount,
                        account_holder = :accountHolder,
                        deduct_day     = :deductDay,
                        deduct_amount  = :deductAmount,
                        cycle_type     = :cycleType,
                        cycle_months   = :cycleMonths,
                        start_date     = :startDate,
                        end_date       = :endDate,
                        agree_yn       = :agreeYn,
                        agree_date     = CASE WHEN :agreeYn = 'Y' AND agree_date IS NULL THEN NOW() ELSE agree_date END,
                        agree_method   = :agreeMethod,
                        status         = :status,
                        memo           = :memo,
                        _modifier_id   = :userId,
                        _modified      = NOW()
                    WHERE id = :id AND spjangcd = :spjangcd
                    """;
            int affected = sqlRunner.execute(sql, param);

            // cms_account_register 동기화 (PENDING 건만 — 이미 SENT/APPROVED는 건드리지 않음)
            if (affected > 0) {
                sqlRunner.execute(
                        """
                        UPDATE cms_account_register
                        SET member_name    = (SELECT member_name FROM cms_member WHERE id = :memberId),
                            bank_code      = :bankCode,
                            bank_account   = :bankAccount,
                            account_holder = :accountHolder,
                            id_number      = :idNumber,
                            member_type    = :memberType,
                            _modified      = NOW()
                        WHERE member_id = :memberId
                          AND spjangcd  = :spjangcd
                          AND status    = 'PENDING'
                          AND ei13_status IN ('PENDING', 'FAILED')
                        """,
                        new MapSqlParameterSource("memberId", id)
                                .addValue("spjangcd", spjangcd)
                                .addValue("bankCode", bankCode)
                                .addValue("bankAccount", bankAccount)
                                .addValue("accountHolder", accountHolder)
                                .addValue("idNumber", idNumber)
                                .addValue("memberType", memberType));
            }

            return affected > 0 ? id : null;
        }
    }

    /** 납부자 삭제 (soft delete) */
    public boolean deleteMember(Long id) {
        String spjangcd = TenantContext.get();
        var param = new MapSqlParameterSource()
                .addValue("id", id)
                .addValue("spjangcd", spjangcd);

        sqlRunner.execute(/* skip_tenant_check */
                "DELETE FROM cms_billing WHERE member_id = :id AND spjangcd = :spjangcd", param);
        sqlRunner.execute(/* skip_tenant_check */
                "DELETE FROM cms_account_register WHERE member_id = :id AND spjangcd = :spjangcd", param);
        return sqlRunner.execute(/* skip_tenant_check */
                "DELETE FROM cms_member WHERE id = :id AND spjangcd = :spjangcd", param) > 0;
    }

    public Map<String, Object> excelUpload(MultipartFile file, String userId) {
        String spjangcd = TenantContext.get();
        int inserted = 0, updated = 0, failed = 0;

        // 은행명 → 코드 맵 로드
        List<Map<String, Object>> bankList = sqlRunner.getRows(/* skip_tenant_check */
                "SELECT bank_code, bank_name FROM cms_bank_code",
                new MapSqlParameterSource());
        Map<String, String> bankNameToCode = new java.util.HashMap<>();
        for (Map<String, Object> b : bankList) {
            bankNameToCode.put(str(b.get("bank_name")), str(b.get("bank_code")));
        }

        try (XSSFWorkbook wb = new XSSFWorkbook(file.getInputStream())) {
            Sheet sheet = wb.getSheetAt(0);

            for (int i = 1; i <= sheet.getLastRowNum(); i++) {
                Row row = sheet.getRow(i);
                if (row == null) continue;

                try {
                    String memberNo      = cellStr(row, 0);
                    String memberTypeNm  = cellStr(row, 1);  // 개인/개인사업자/법인
                    String memberName    = cellStr(row, 2);
                    String idNumber      = cellStr(row, 3);
                    String phone         = cellStr(row, 4);
                    String email         = cellStr(row, 5);
                    String bankName      = cellStr(row, 6);  // 은행명
                    String bankAccount   = cellStr(row, 7).replaceAll("-", "");
                    String accountHolder = cellStr(row, 8);
                    String deductDay     = cellStr(row, 9);
                    String deductAmount  = cellStr(row, 10);
                    String startDate     = cellStr(row, 11).replaceAll("-", "");
                    String endDate       = cellStr(row, 12).replaceAll("-", "");
                    String cycleMonths   = cellStr(row, 13); // 비어있으면 REGULAR
                    String memo          = cellStr(row, 14);

                    if (!StringUtils.hasText(memberName)) continue;

                    // 구분 변환
                    String memberType = switch (memberTypeNm) {
                        case "개인사업자" -> "S";
                        case "법인"      -> "C";
                        default          -> "P";
                    };

                    // 은행코드 변환
                    String bankCode = bankNameToCode.get(bankName);
                    if (!StringUtils.hasText(bankCode)) {
                        log.error("[ExcelUpload] {}행 은행명 없음: {}", i + 1, bankName);
                        failed++;
                        continue;
                    }

                    // 약정일 변환 (말일 → 99)
                    if ("말일".equals(deductDay)) deductDay = "99";
                    else if (deductDay.length() == 1) deductDay = "0" + deductDay; // 1 → 01

                    // 결제주기 — 결제월 있으면 IRREGULAR, 없으면 REGULAR
                    String cycleType = StringUtils.hasText(cycleMonths) ? "IRREGULAR" : "REGULAR";
                    if ("REGULAR".equals(cycleType)) cycleMonths = null;

                    if (!StringUtils.hasText(endDate)) endDate = "99991231";

                    var param = new MapSqlParameterSource();
                    param.addValue("spjangcd",      spjangcd);
                    param.addValue("memberType",    memberType);
                    param.addValue("memberName",    memberName);
                    param.addValue("idNumber",      idNumber);
                    param.addValue("phone",         phone);
                    param.addValue("email",         email);
                    param.addValue("bankCode",      bankCode);
                    param.addValue("bankAccount",   bankAccount);
                    param.addValue("accountHolder", accountHolder);
                    param.addValue("deductDay",     deductDay);
                    param.addValue("deductAmount",  StringUtils.hasText(deductAmount) ? Long.parseLong(deductAmount.replaceAll(",", "")) : 0);
                    param.addValue("startDate",     startDate);
                    param.addValue("endDate",       endDate);
                    param.addValue("cycleType",     cycleType);
                    param.addValue("cycleMonths",   cycleMonths);
                    param.addValue("memo",          memo);
                    param.addValue("userId",        userId);

                    if (StringUtils.hasText(memberNo)) {
                        // UPDATE
                        param.addValue("memberNo", memberNo);
                        int cnt = sqlRunner.execute(/* skip_tenant_check */
                                """
                                UPDATE cms_member SET
                                    member_type = :memberType, member_name = :memberName,
                                    id_number = :idNumber, phone = :phone, email = :email,
                                    bank_code = :bankCode, bank_account = :bankAccount,
                                    account_holder = :accountHolder, deduct_day = :deductDay,
                                    deduct_amount = :deductAmount, start_date = :startDate,
                                    end_date = :endDate, cycle_type = :cycleType,
                                    cycle_months = :cycleMonths,
                                    memo = :memo, _modifier_id = :userId, _modified = NOW()
                                WHERE member_no = :memberNo AND spjangcd = :spjangcd
                                """, param);
                        if (cnt > 0) updated++;
                        else failed++;
                    } else {
                        // INSERT
                        String newMemberNo = generateMemberNo(spjangcd);
                        param.addValue("memberNo", newMemberNo);
                        Map<String, Object> insertedRow = sqlRunner.getRow(/* skip_tenant_check */
                                """
                                INSERT INTO cms_member (
                                    spjangcd, member_no, member_type, member_name,
                                    id_number, phone, email, bank_code, bank_account,
                                    account_holder, deduct_day, deduct_amount,
                                    start_date, end_date, cycle_type, cycle_months,
                                    status, memo, agree_yn,
                                    _creater_id, _created, _modifier_id, _modified
                                ) VALUES (
                                    :spjangcd, :memberNo, :memberType, :memberName,
                                    :idNumber, :phone, :email, :bankCode, :bankAccount,
                                    :accountHolder, :deductDay, :deductAmount,
                                    :startDate, :endDate, :cycleType, :cycleMonths,
                                    'ACTIVE', :memo, 'N',
                                    :userId, NOW(), :userId, NOW()
                                ) RETURNING id
                                """, param);
                        Long savedId = ((Number) insertedRow.get("id")).longValue();
                        cmsAccountRegisterService.save(savedId, "1", null, null, userId);
                        inserted++;
                    }
                } catch (Exception e) {
                    log.error("[ExcelUpload] {}행 처리 실패: {}", i + 1, e.getMessage());
                    failed++;
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("엑셀 파일 파싱 실패: " + e.getMessage());
        }

        return Map.of("inserted", inserted, "updated", updated, "failed", failed);
    }

    public void downloadTemplate(HttpServletResponse response) throws Exception {
        try (Workbook wb = new XSSFWorkbook()) {
            Sheet sheet = wb.createSheet("납부자");

            // 헤더 스타일
            CellStyle headerStyle = wb.createCellStyle();
            headerStyle.setFillForegroundColor(IndexedColors.LIGHT_CORNFLOWER_BLUE.getIndex());
            headerStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);
            Font headerFont = wb.createFont();
            headerFont.setBold(true);
            headerStyle.setFont(headerFont);

            // 필수항목 스타일 (빨간 글씨)
            CellStyle requiredStyle = wb.createCellStyle();
            requiredStyle.setFillForegroundColor(IndexedColors.LIGHT_CORNFLOWER_BLUE.getIndex());
            requiredStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);
            Font requiredFont = wb.createFont();
            requiredFont.setBold(true);
            requiredFont.setColor(IndexedColors.RED.getIndex());
            requiredStyle.setFont(requiredFont);

            // 예시 스타일
            CellStyle exampleStyle = wb.createCellStyle();
            exampleStyle.setFillForegroundColor(IndexedColors.GREY_25_PERCENT.getIndex());
            exampleStyle.setFillPattern(FillPatternType.SOLID_FOREGROUND);

            String[] headers = {
                    "납부자번호(수정시입력)", "구분(개인/개인사업자/법인)*", "납부자명*",
                    "생년월일/사업자번호", "연락처", "이메일",
                    "은행명*", "계좌번호*", "예금주명*",
                    "약정일(1~31, 말일)*", "출금금액*",
                    "청구시작일(YYYY-MM-DD)*", "청구종료일(YYYY-MM-DD, 빈칸=무제한)",
                    "결제월(비어있으면매월, 예:1,3,6,9)", "메모"
            };

            int[] requiredCols = {1, 2, 6, 7, 8, 9, 10, 11};
            java.util.Set<Integer> requiredSet = new java.util.HashSet<>();
            for (int c : requiredCols) requiredSet.add(c);

            Row headerRow = sheet.createRow(0);
            for (int i = 0; i < headers.length; i++) {
                Cell cell = headerRow.createCell(i);
                cell.setCellValue(headers[i]);
                cell.setCellStyle(requiredSet.contains(i) ? requiredStyle : headerStyle);
                sheet.setColumnWidth(i, 6000);
            }

            // 예시 행 1 - 신규 (매월 정기)
            Object[] ex1 = {
                    "", "개인", "홍길동", "19900101", "01012345678", "hong@test.com",
                    "기업은행", "12345678901234", "홍길동",
                    "25", "100000", "2026-01-01", "", "", "예시-매월정기"
            };
            // 예시 행 2 - 신규 (비정기)
            Object[] ex2 = {
                    "", "법인", "테스트법인", "1234567890", "0212345678", "",
                    "국민은행", "98765432109876", "테스트법인",
                    "말일", "500000", "2026-01-01", "2027-12-31", "1,3,6,9", "예시-비정기"
            };
            // 예시 행 3 - 수정
            Object[] ex3 = {
                    "ZZ000001", "개인사업자", "김사업자", "1234567890", "01098765432", "",
                    "신한은행", "11223344556677", "김사업자",
                    "10", "200000", "2026-01-01", "", "", "예시-수정"
            };

            Object[][] examples = {ex1, ex2, ex3};
            for (int r = 0; r < examples.length; r++) {
                Row row = sheet.createRow(r + 1);
                for (int c = 0; c < examples[r].length; c++) {
                    Cell cell = row.createCell(c);
                    cell.setCellStyle(exampleStyle);
                    if (examples[r][c] != null) cell.setCellValue(examples[r][c].toString());
                }
            }

            response.setContentType("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
            response.setHeader("Content-Disposition", "attachment; filename=member_template.xlsx");
            wb.write(response.getOutputStream());
        }
    }

    private String cellStr(Row row, int col) {
        Cell cell = row.getCell(col);
        if (cell == null) return "";
        return switch (cell.getCellType()) {
            case STRING  -> cell.getStringCellValue().trim();
            case NUMERIC -> {
                if (DateUtil.isCellDateFormatted(cell)) {
                    yield new java.text.SimpleDateFormat("yyyyMMdd").format(cell.getDateCellValue());
                }
                yield String.valueOf((long) cell.getNumericCellValue());
            }
            case BOOLEAN -> String.valueOf(cell.getBooleanCellValue());
            default -> "";
        };
    }
    private String generateMemberNo(String spjangcd) {
        Map<String, Object> seqRow = sqlRunner.getRow(
                """
                SELECT COALESCE(MAX(CAST(SUBSTRING(member_no, LENGTH(:spjangcd) + 1) AS INTEGER)), 0) + 1 AS next_seq
                FROM cms_member
                WHERE spjangcd = :spjangcd
                  AND member_no ~ ('^' || :spjangcd || '[0-9]+$')
                """,
                new MapSqlParameterSource("spjangcd", spjangcd));
        int nextSeq = seqRow != null ? ((Number) seqRow.get("next_seq")).intValue() : 1;
        return spjangcd + String.format("%06d", nextSeq);
    }


}
