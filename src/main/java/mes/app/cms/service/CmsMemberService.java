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
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
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

    private String str(Object v) {
        return v != null ? v.toString() : "";
    }

    /**
     * 납부자 목록 조회
     */
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
                     , m.pause_start_date
                     , m.pause_end_date
                     , m.pause_reason
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

    /**
     * 납부자 단건 조회
     */
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
                     , m.pause_start_date
                     , m.pause_end_date
                     , m.pause_reason
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

    /**
     * 납부자 저장 (신규/수정)
     */
    public Long saveMember(Long id, String memberType, String memberName, String memberNo,
                           String idNumber, String phone, String email,
                           String zipcd, String adresa, String adresb,
                           String bankCode, String bankAccount, String accountHolder,
                           String deductDay, Long deductAmount,
                           String cycleType, String cycleMonths,
                           String startDate, String endDate,
                           String pauseStartDate, String pauseEndDate, String pauseReason,  // 추가됨
                           String agreeYn, String agreeMethod,
                           String status, String memo,
                           String userId) {

        String spjangcd = TenantContext.get();

        // 중지 기간 유효성 검사
        if (StringUtils.hasText(pauseStartDate) && StringUtils.hasText(pauseEndDate)) {
            validatePausePeriod(pauseStartDate, pauseEndDate);
        } else if (StringUtils.hasText(pauseStartDate) || StringUtils.hasText(pauseEndDate)) {
            // 시작일과 종료일 중 하나만 입력된 경우
            throw new IllegalArgumentException("중지 기간은 시작일과 종료일을 모두 입력하거나 비워주세요.");
        }

        // 신규 등록
        if (id == null) {
            memberNo = generateMemberNo(spjangcd);

            MapSqlParameterSource param = new MapSqlParameterSource();
            param.addValue("spjangcd",      spjangcd);
            param.addValue("memberType",    StringUtils.hasText(memberType) ? memberType : "C");
            param.addValue("memberName",    memberName);
            param.addValue("memberNo",      memberNo);
            param.addValue("idNumber",      idNumber);
            param.addValue("phone",         phone);
            param.addValue("email",         email);
            param.addValue("zipcd",         zipcd);
            param.addValue("adresa",        adresa);
            param.addValue("adresb",        adresb);
            param.addValue("bankCode",      bankCode);
            param.addValue("bankAccount",   bankAccount);
            param.addValue("accountHolder", accountHolder);
            param.addValue("deductDay",     deductDay);
            param.addValue("deductAmount",  deductAmount);
            param.addValue("cycleType",     cycleType);
            param.addValue("cycleMonths",   cycleMonths);
            param.addValue("startDate",     startDate);
            param.addValue("endDate",       endDate);
            param.addValue("pauseStartDate",  pauseStartDate);  // 추가됨
            param.addValue("pauseEndDate",    pauseEndDate);    // 추가됨
            param.addValue("pauseReason",     pauseReason);     // 추가됨
            param.addValue("agreeYn",       agreeYn);
            param.addValue("agreeMethod",   agreeMethod);
            param.addValue("status",        StringUtils.hasText(status) ? status : "ACTIVE");
            param.addValue("memo",          memo);
            param.addValue("userId",        userId);

            String insertSql = """
                    INSERT INTO cms_member (
                        spjangcd, member_type, member_name, member_no,
                        id_number, phone, email,
                        zipcd, adresa, adresb,
                        bank_code, bank_account, account_holder,
                        deduct_day, deduct_amount,
                        cycle_type, cycle_months,
                        start_date, end_date,
                        pause_start_date, pause_end_date, pause_reason,
                        agree_yn, agree_method, status, memo,
                        _creater_id, _created, _modifier_id, _modified
                    ) VALUES (
                        :spjangcd, :memberType, :memberName, :memberNo,
                        :idNumber, :phone, :email,
                        :zipcd, :adresa, :adresb,
                        :bankCode, :bankAccount, :accountHolder,
                        :deductDay, :deductAmount,
                        :cycleType, :cycleMonths,
                        :startDate, :endDate,
                        :pauseStartDate, :pauseEndDate, :pauseReason,
                        :agreeYn, :agreeMethod, :status, :memo,
                        :userId, NOW(), :userId, NOW()
                    )
                    """;

            sqlRunner.execute(insertSql, param);

            // 신규 등록 로그
            log.info("[CMS] 납부자 신규 등록 - ID: {}, 이름: {}, 중지기간: {} ~ {}",
                    memberNo, memberName, pauseStartDate, pauseEndDate);

            Map<String, Object> result = sqlRunner.getRow(
                    "SELECT id FROM cms_member WHERE spjangcd = :spjangcd AND member_no = :memberNo",
                    new MapSqlParameterSource("spjangcd", spjangcd).addValue("memberNo", memberNo));

            return result != null ? ((Number) result.get("id")).longValue() : null;
        }

        // 수정
        else {
            MapSqlParameterSource param = new MapSqlParameterSource();
            param.addValue("id",            id);
            param.addValue("memberType",    StringUtils.hasText(memberType) ? memberType : "C");
            param.addValue("memberName",    memberName);
            param.addValue("idNumber",      idNumber);
            param.addValue("phone",         phone);
            param.addValue("email",         email);
            param.addValue("zipcd",         zipcd);
            param.addValue("adresa",        adresa);
            param.addValue("adresb",        adresb);
            param.addValue("bankCode",      bankCode);
            param.addValue("bankAccount",   bankAccount);
            param.addValue("accountHolder", accountHolder);
            param.addValue("deductDay",     deductDay);
            param.addValue("deductAmount",  deductAmount);
            param.addValue("cycleType",     cycleType);
            param.addValue("cycleMonths",   cycleMonths);
            param.addValue("startDate",     startDate);
            param.addValue("endDate",       endDate);
            param.addValue("pauseStartDate",  pauseStartDate);  // 추가됨
            param.addValue("pauseEndDate",    pauseEndDate);    // 추가됨
            param.addValue("pauseReason",     pauseReason);     // 추가됨
            param.addValue("agreeYn",       agreeYn);
            param.addValue("agreeMethod",   agreeMethod);
            param.addValue("status",        status);
            param.addValue("memo",          memo);
            param.addValue("userId",        userId);
            param.addValue("spjangcd",      spjangcd);

            String updateSql = """
                    UPDATE cms_member SET
                        member_type    = :memberType,
                        member_name    = :memberName,
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
                        pause_start_date = :pauseStartDate,
                        pause_end_date   = :pauseEndDate,
                        pause_reason     = :pauseReason,
                        agree_yn       = :agreeYn,
                        agree_method   = :agreeMethod,
                        status         = :status,
                        memo           = :memo,
                        _modifier_id   = :userId,
                        _modified      = NOW()
                    WHERE id = :id AND spjangcd = :spjangcd
                    """;

            sqlRunner.execute(updateSql, param);

            return id;
        }
    }

    /**
     * 중지 기간 유효성 검사 (추가됨)
     */
    private void validatePausePeriod(String pauseStartDate, String pauseEndDate) {
        try {
            LocalDate start = LocalDate.parse(pauseStartDate, DateTimeFormatter.ofPattern("yyyyMMdd"));
            LocalDate end = LocalDate.parse(pauseEndDate, DateTimeFormatter.ofPattern("yyyyMMdd"));

            if (start.isAfter(end)) {
                throw new IllegalArgumentException("중지 종료일은 시작일보다 같거나 이후여야 합니다.");
            }

            // 과거 날짜 경고 (로그에만 남김)
            LocalDate today = LocalDate.now();
            if (end.isBefore(today)) {
                log.warn("[CMS] 이미 지난 중지 기간이 설정되었습니다 - 시작: {}, 종료: {}",
                        pauseStartDate, pauseEndDate);
            }
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException("날짜 형식이 올바르지 않습니다. YYYYMMDD 형식으로 입력해주세요.");
        }
    }

    /**
     * 특정 날짜가 중지 기간인지 확인 (청구 생성 시 사용)
     */
    public boolean isPausedOnDate(Long memberId, LocalDate date) {
        Map<String, Object> member = getMember(memberId);
        if (member == null) return false;

        String pauseStartDate = str(member.get("pause_start_date"));
        String pauseEndDate = str(member.get("pause_end_date"));

        if (!StringUtils.hasText(pauseStartDate) || !StringUtils.hasText(pauseEndDate)) {
            return false;
        }

        try {
            LocalDate start = LocalDate.parse(pauseStartDate, DateTimeFormatter.ofPattern("yyyyMMdd"));
            LocalDate end = LocalDate.parse(pauseEndDate, DateTimeFormatter.ofPattern("yyyyMMdd"));

            return !date.isBefore(start) && !date.isAfter(end);
        } catch (Exception e) {
            log.error("[CMS] 중지 기간 확인 중 오류 - memberId: {}, date: {}", memberId, date, e);
            return false;
        }
    }

    /**
     * 납부자 삭제 (soft delete)
     */
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
                    String memberNo = cellStr(row, 0);
                    String memberTypeNm = cellStr(row, 1);  // 개인/개인사업자/법인
                    String memberName = cellStr(row, 2);
                    String idNumber = cellStr(row, 3);
                    String phone = cellStr(row, 4);
                    String email = cellStr(row, 5);
                    String bankName = cellStr(row, 6);  // 은행명
                    String bankAccount = cellStr(row, 7).replaceAll("-", "");
                    String accountHolder = cellStr(row, 8);
                    String deductDay = cellStr(row, 9);
                    String deductAmount = cellStr(row, 10);
                    String startDate = cellStr(row, 11).replaceAll("-", "");
                    String endDate = cellStr(row, 12).replaceAll("-", "");
                    String cycleMonths = cellStr(row, 13); // 비어있으면 REGULAR
                    String memo = cellStr(row, 14);

                    if (!StringUtils.hasText(memberName)) continue;

                    // 구분 변환
                    String memberType = switch (memberTypeNm) {
                        case "개인사업자" -> "S";
                        case "법인" -> "C";
                        default -> "P";
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
                    param.addValue("spjangcd", spjangcd);
                    param.addValue("memberType", memberType);
                    param.addValue("memberName", memberName);
                    param.addValue("idNumber", idNumber);
                    param.addValue("phone", phone);
                    param.addValue("email", email);
                    param.addValue("bankCode", bankCode);
                    param.addValue("bankAccount", bankAccount);
                    param.addValue("accountHolder", accountHolder);
                    param.addValue("deductDay", deductDay);
                    param.addValue("deductAmount", StringUtils.hasText(deductAmount) ? Long.parseLong(deductAmount.replaceAll(",", "")) : 0);
                    param.addValue("startDate", startDate);
                    param.addValue("endDate", endDate);
                    param.addValue("cycleType", cycleType);
                    param.addValue("cycleMonths", cycleMonths);
                    param.addValue("memo", memo);
                    param.addValue("userId", userId);

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
            case STRING -> cell.getStringCellValue().trim();
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

    /**
     * ERP(MS SQL) TB_XCLIENT + TB_E101 → cms_member 동기화
     * - TB_XCLIENT(allchk='1') INNER JOIN TB_E101(최신 계약) 기준
     * - member_no(cltcd) 기준 upsert
     * - 금액/약정일/청구기간/출금월 모두 동기화
     */
    public Map<String, Object> syncFromErp(String spjangcd, String userId) {
        int inserted = 0, updated = 0, failed = 0;

        // 1. ERP 접속정보 + custcd + ms_spjangcd 조회
        Map<String, Object> erp = sqlRunner.getRow(/* skip_tenant_check */
                "SELECT host, port, db_name, username, password, custcd, ms_spjangcd FROM tb_xa012_erp WHERE spjangcd = :spjangcd",
                new MapSqlParameterSource("spjangcd", spjangcd));

        if (erp == null) throw new IllegalStateException("ERP 접속정보가 없습니다.");

        String custcd     = str(erp.get("custcd"));
        String msSpjangcd = str(erp.get("ms_spjangcd"));
        if (custcd.isEmpty()) throw new IllegalStateException("업체코드(custcd)가 설정되어 있지 않습니다.");

        // 2. CMS 승인 여부 확인 (ms_spjangcd 기준 매칭)
        Map<String, Object> cms = sqlRunner.getRow(/* skip_tenant_check */
                "SELECT is_normal_status FROM tb_xa012_cms WHERE spjangcd = :spjangcd AND ms_spjangcd IS NOT DISTINCT FROM :msSpjangcd",
                new MapSqlParameterSource("spjangcd", spjangcd)
                        .addValue("msSpjangcd", msSpjangcd.isEmpty() ? null : msSpjangcd));

        if (cms == null || !Boolean.TRUE.equals(cms.get("is_normal_status"))) {
            throw new IllegalStateException("CMS 서비스 상태가 승인이 아닙니다.");
        }

        // 은행코드 매핑 (TB_XBANK.bnkcode → cms_bank_code.bank_code)
        Map<String, String> bnkCodeMap = new java.util.HashMap<>();
        bnkCodeMap.put("002", "002"); // 산업은행
        bnkCodeMap.put("003", "003"); // 기업은행
        bnkCodeMap.put("007", "007"); // 수협
        bnkCodeMap.put("008", "008"); // 수출입은행
        bnkCodeMap.put("012", "011"); // 농협 → NH농협
        bnkCodeMap.put("019", "004"); // 국민은행
        bnkCodeMap.put("020", "020"); // 우리은행
        bnkCodeMap.put("023", "023"); // SC제일은행
        bnkCodeMap.put("027", "027"); // 한국씨티은행
        bnkCodeMap.put("032", "032"); // 부산은행
        bnkCodeMap.put("034", "034"); // 광주은행
        bnkCodeMap.put("035", "035"); // 제주은행
        bnkCodeMap.put("037", "037"); // 전북은행
        bnkCodeMap.put("039", "039"); // 경남은행
        bnkCodeMap.put("045", "045"); // 새마을금고
        bnkCodeMap.put("047", "048"); // 신협
        bnkCodeMap.put("050", "050"); // 저축은행
        bnkCodeMap.put("064", "064"); // 산림조합
        bnkCodeMap.put("071", "071"); // 우체국
        bnkCodeMap.put("081", "081"); // 하나은행
        bnkCodeMap.put("088", "088"); // 신한은행
        bnkCodeMap.put("089", "089"); // 케이뱅크
        bnkCodeMap.put("090", "090"); // 카카오뱅크
        bnkCodeMap.put("092", "092"); // 토스뱅크

        // 3. MS DB 연결
        String url = String.format("jdbc:sqlserver://%s:%s;databaseName=%s;encrypt=false",
                str(erp.get("host")), str(erp.get("port")), str(erp.get("db_name")));

        try { Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver"); }
        catch (ClassNotFoundException e) { throw new IllegalStateException("MSSQL 드라이버 없음"); }

        try (java.sql.Connection conn = java.sql.DriverManager.getConnection(
                url, str(erp.get("username")), str(erp.get("password")))) {

            // 4. TB_XCLIENT JOIN TB_E101 (최신 계약 기준)
            String sql = """
                SELECT
                    C.cltcd       AS member_no,
                    C.cltnm       AS member_name,
                    C.cmsrnum     AS id_number,
                    B.bnkcode     AS bank_code,
                    C.accnum      AS bank_account,
                    C.hptelnum    AS phone,
                    C.agneremail  AS email,
                    C.cltadres    AS adresa,
                    C.zipcd       AS zipcd,
                    E.amt         AS deduct_amount,
                    E.dedate      AS deduct_day,
                    E.stdate      AS start_date,
                    E.enddate     AS end_date,
                    E.delmon1, E.delmon2, E.delmon3, E.delmon4,
                    E.delmon5, E.delmon6, E.delmon7, E.delmon8,
                    E.delmon9, E.delmon10, E.delmon11, E.delmon12,
                    (SELECT TOP 1 SPDATE FROM TB_CMSEB13
                     WHERE CLTCD = C.cltcd AND ACTCD IS NULL
                     ORDER BY SPDATE DESC) AS agree_date,
                    (SELECT TOP 1 BANKCLTCD FROM TB_CMSEB13
                     WHERE CLTCD = C.cltcd AND ACTCD IS NULL
                     ORDER BY SPDATE DESC) AS bankcltcd
                FROM TB_XCLIENT C WITH(NOLOCK)
                INNER JOIN TB_XBANK B WITH(NOLOCK) ON C.bankcd = B.bankcd
                INNER JOIN TB_E101 E WITH(NOLOCK)
                    ON C.cltcd = E.cltcd AND E.custcd = ?
                WHERE C.custcd = ?
                AND C.allchk = '1'
                AND E.stdate = (
                    SELECT MAX(stdate) FROM TB_E101
                    WHERE cltcd = E.cltcd AND custcd = ?
                )
                """;

            try (java.sql.PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, custcd);
                ps.setString(2, custcd);
                ps.setString(3, custcd);

                try (java.sql.ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        try {
                            String memberName    = rs.getString("member_name");
                            String idNumber      = rs.getString("id_number");
                            String bnkCode       = rs.getString("bank_code"); // TB_XBANK.bnkcode
                            String bankCode      = bnkCodeMap.getOrDefault(bnkCode, bnkCode); // cms_bank_code 변환
                            String bankAccount   = rs.getString("bank_account");
                            String phone         = rs.getString("phone");
                            String email         = rs.getString("email");
                            String adresa        = rs.getString("adresa");
                            String zipcd2        = rs.getString("zipcd");
                            Long   deductAmount  = rs.getLong("deduct_amount");
                            String deductDay     = rs.getString("deduct_day");
                            String startDate     = cleanDate(rs.getString("start_date"));
                            String endDate       = cleanDate(rs.getString("end_date"));
                            String agreeDate = cleanDate(rs.getString("agree_date"));
                            String cltcd     = rs.getString("member_no");  // TB_XCLIENT.cltcd
                            String bankcltcd = rs.getString("bankcltcd");
                            String agreeYn;
                            String memberNo;

                            if (StringUtils.hasText(bankcltcd)) {
                                // TB_CMSEB13 인증 완료 → BANKCLTCD를 member_no로
                                memberNo = bankcltcd;
                                agreeYn  = "Y";
                            } else {
                                // 인증 안 됨 → SaaS 채번
                                memberNo = generateMemberNo(spjangcd);
                                agreeYn  = "N";
                            }

                            if (endDate == null || endDate.isEmpty()) endDate = "99991231";
                            if (bankAccount != null) bankAccount = bankAccount.replaceAll("-", "").trim();

                            // deduct_day 2자리 패딩 (24 → "24", 9 → "09")
                            if (deductDay != null && !deductDay.trim().isEmpty()) {
                                try {
                                    deductDay = String.format("%02d", Integer.parseInt(deductDay.trim()));
                                } catch (NumberFormatException ignored) {}
                            }

                            // cycle_months: delmon1~12 중 값 있는 것 콤마 연결
                            List<String> months = new java.util.ArrayList<>();
                            for (int i = 1; i <= 12; i++) {
                                String mon = rs.getString("delmon" + i);
                                if (mon != null && !mon.trim().isEmpty()) {
                                    months.add(String.valueOf(Integer.parseInt(mon.trim())));
                                }
                            }
                            // 1~12 전부 있으면 REGULAR, 일부만 있으면 IRREGULAR
                            String cycleMonths = months.isEmpty() ? null : String.join(",", months);
                            String cycleType   = (months.size() == 12) ? "REGULAR" : "IRREGULAR";
                            if ("REGULAR".equals(cycleType)) cycleMonths = null;

                            // 기존 member 조회
                            Map<String, Object> existing = sqlRunner.getRow(/* skip_tenant_check */
                                    "SELECT id FROM cms_member WHERE spjangcd = :spjangcd AND cltcd = :cltcd",
                                    new MapSqlParameterSource("spjangcd", spjangcd).addValue("cltcd", cltcd));

                            MapSqlParameterSource p = new MapSqlParameterSource();
                            p.addValue("spjangcd",      spjangcd);
                            p.addValue("memberNo",      memberNo);
                            p.addValue("cltcd",    cltcd);
                            p.addValue("agreeYn",  agreeYn);
                            p.addValue("memberName",    memberName);
                            p.addValue("memberType",    "C");
                            p.addValue("idNumber",      idNumber);
                            p.addValue("bankCode",      bankCode);
                            p.addValue("bankAccount",   bankAccount);
                            p.addValue("phone",         phone);
                            p.addValue("email",         email);
                            p.addValue("adresa",        adresa);
                            p.addValue("zipcd",         zipcd2);
                            p.addValue("deductAmount",  deductAmount);
                            p.addValue("deductDay",     deductDay);
                            p.addValue("startDate",     startDate);
                            p.addValue("endDate",       endDate);
                            p.addValue("cycleType",     cycleType);
                            p.addValue("cycleMonths",   cycleMonths);
                            p.addValue("agreeYn",   agreeYn);
                            p.addValue("bankcltcd", bankcltcd);
                            p.addValue("userId",        userId);
                            p.addValue("agreeDate", agreeDate);

                            if (existing == null) {
                                // member_no = MS cltcd 그대로 사용
                                sqlRunner.execute(/* skip_tenant_check */
                                        """
                                        INSERT INTO cms_member (
                                            spjangcd, member_no, member_type, member_name,
                                            id_number, bank_code, bank_account,
                                            phone, email, adresa, zipcd,
                                            deduct_amount, deduct_day,
                                            start_date, end_date,
                                            cycle_type, cycle_months,
                                            agree_yn, agree_date, cltcd, status,
                                            _creater_id, _created, _modifier_id, _modified
                                        ) VALUES (
                                            :spjangcd, :memberNo, :memberType, :memberName,
                                            :idNumber, :bankCode, :bankAccount,
                                            :phone, :email, :adresa, :zipcd,
                                            :deductAmount, :deductDay,
                                            :startDate, :endDate,
                                            :cycleType, :cycleMonths,
                                            :agreeYn, CAST(:agreeDate AS DATE), :cltcd, 'ACTIVE',
                                            :userId, NOW(), :userId, NOW()
                                        )
                                        """, p);
                                inserted++;
                            } else {
                                // 업데이트 — 계좌정보 + 계약정보 모두 갱신
                                sqlRunner.execute(/* skip_tenant_check */
                                        """
                                        UPDATE cms_member SET
                                            member_name    = :memberName,
                                            member_no      = :memberNo,
                                            id_number      = :idNumber,
                                            bank_code      = :bankCode,
                                            bank_account   = :bankAccount,
                                            phone          = :phone,
                                            email          = :email,
                                            adresa         = :adresa,
                                            zipcd          = :zipcd,
                                            deduct_amount  = :deductAmount,
                                            deduct_day     = :deductDay,
                                            start_date     = :startDate,
                                            end_date       = :endDate,
                                            cycle_type     = :cycleType,
                                            cycle_months   = :cycleMonths,
                                            agree_yn       = :agreeYn,
                                            agree_date     = COALESCE(agree_date, CAST(:agreeDate AS DATE)),
                                            cltcd          = :cltcd,
                                            _modifier_id   = :userId,
                                            _modified      = NOW(),
                                            bankcltcd = COALESCE(:bankcltcd, bankcltcd),
                                            agree_yn  = :agreeYn
                                        WHERE spjangcd = :spjangcd AND cltcd = :cltcd
                                        """, p);
                                updated++;
                            }
                        } catch (Exception e) {
                            log.warn("[ERP동기화] 행 처리 실패: {}", e.getMessage());
                            failed++;
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error("[ERP동기화] 실패 spjangcd={}: {}", spjangcd, e.getMessage());
            throw new IllegalStateException("MS DB 연결 또는 조회 실패: " + e.getMessage());
        }

        log.info("[ERP동기화] 완료 spjangcd={} 신규={} 수정={} 실패={}", spjangcd, inserted, updated, failed);
        return Map.of("inserted", inserted, "updated", updated, "failed", failed);
    }

    private String cleanDate(String date) {
        if (date == null || date.trim().isEmpty()) return null;
        String cleaned = date.replaceAll("[-.]", "").trim();
        // 6자리면 20XX로 변환
        if (cleaned.length() == 6) cleaned = "20" + cleaned;
        return cleaned.length() >= 8 ? cleaned.substring(0, 8) : cleaned;
    }
}