package mes.app.approval.service;

import mes.domain.services.SqlRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import javax.transaction.Transactional;
import java.io.File;
import java.util.List;
import java.util.Map;

@Service
public class ApprovalService {

    @Autowired
    SqlRunner sqlRunner;

    //결재라인등록 그리드 리스트 불러오기
    public List<Map<String, Object>> getCheckPaymentList(int personid, String papercd, String spjangcd) {
        MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("personid", personid);
        String sql = """
                select
                e.*,
                p."Name",
                s."Value" as gubunnm
                from TB_E064 e
                LEFT JOIN person p ON e.kcpersonid = p.id
                LEFT JOIN sys_code s ON e.gubun = s."Code"
                WHERE 1=1
                AND s."CodeType" = 'approval_status'
                AND e.personid = :personid
                """;
        if(papercd != null && !papercd.isEmpty()) {
            dicParam.addValue("papercd", papercd);
            sql += " AND e.papercd = :papercd";
        }
        if(spjangcd != null && !spjangcd.isEmpty()) {
            dicParam.addValue("spjangcd", spjangcd);
            sql += " AND e.spjangcd = :spjangcd";
        }
        sql += " order by e.seq ASC";
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);
        return items;
    }

    //결재라인등록 사원 그리드 리스트 불러오기
    public List<Map<String, Object>> getListPapercd(String papercd, String spjangcd) {
        MapSqlParameterSource dicParam = new MapSqlParameterSource();

        String sql = """
                select
                e.*,
                s."Value" as papernm,
                p."Name"
                from TB_E063 e
                LEFT JOIN sys_code s ON e.papercd = s."Code"
                LEFT JOIN person p ON e.personid = p.id
                WHERE 1=1
                AND s."CodeType" = 'appr_doc'
                """;
            dicParam.addValue("papercd", papercd);
            sql += " AND e.papercd = :papercd";
        if(spjangcd != null && !spjangcd.isEmpty()) {
            dicParam.addValue("spjangcd", spjangcd);
            sql += " AND e.spjangcd = :spjangcd";
        }
        sql += " order by e.papercd ASC";
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);
        return items;
    }

    // 문서코드 옵션 불러오기
    public List<Map<String, Object>> getComcd() {

        MapSqlParameterSource dicParam = new MapSqlParameterSource();

        String sql = """
                SELECT com_cls,
                         com_code,
                       com_cls + com_code as asmc,
                         com_cnam,
                         com_rem1,
                         com_rem2,
                         com_order
                    FROM tb_ca510
                 WHERE com_cls = '620'
                   AND com_code <> '00'
                
                """;

        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);
        return items;
    }
    // 문서에 따른 결재자 옵션 불러오기
    public List<Map<String, Object>> getKcperid() {

        MapSqlParameterSource dicParam = new MapSqlParameterSource();
        String sql = """
                SELECT Right(perid, Len(perid) - 1) cd  ,
                 pernm           cdnm,
                 b.divinm        arg2
                  FROM TB_JA001 WITH(NOLOCK)
                 join TB_JC002 b ON  b.divicd = tb_ja001.divicd and b.spjangcd = tb_ja001.spjangcd
                 WHERE rtclafi = '001'
                """;

        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);
        return items;
    }
    // 공통코드 구분 옵션 조회
    public String getGubuncd(String gubuncd) {
        MapSqlParameterSource dicParam = new MapSqlParameterSource();

        String sql = """
                SELECT Value
                FROM user_code
                WHERE Parent_id = 333
                AND Code = :gubuncd
                """;
        dicParam.addValue("gubuncd", gubuncd);
        Map<String, Object> userInfo = this.sqlRunner.getRow(sql, dicParam);
        String gubunnm = (String) userInfo.get("Value");
        return gubunnm;
    }

    // username으로 cltcd, cltnm, saupnum, custcd 가지고 오기
    public Map<String, Object> getUserInfo(String username) {
        MapSqlParameterSource dicParam = new MapSqlParameterSource();

        String sql = """
                select personid,
                       firstname
                FROM auth_user
                WHERE username = :username
                """;
        dicParam.addValue("username", username);
        Map<String, Object> userInfo = this.sqlRunner.getRow(sql, dicParam);
        return userInfo;
    }
    // xusers 데이터 가져오기
    public Map<String, Object> getMyInfo(String username) {
        MapSqlParameterSource dicParam = new MapSqlParameterSource();

        String sql = """
                SELECT *
                FROM tb_xusers
                WHERE userid = :username
                """;
        dicParam.addValue("username", username);
        Map<String, Object> userInfo = this.sqlRunner.getRow(sql, dicParam);
        return userInfo;
    }
    // 공통코드 데이터 불러오기
    @Transactional
    public List<Map<String, Object>> findByParentId(Integer Parent_id){
        MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("Parent_id", Parent_id);

        String sql = """
				select Code, "Value" from user_code where "Parent_id" = :Parent_id order by id;
				""";
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);
        return items;
    }

    // 사용자 사원코드 조회(맨앞 'p'제거 필요)
    public String getPerid(String username) {
        MapSqlParameterSource dicParam = new MapSqlParameterSource();

        String sql = """
                SELECT perid
                FROM tb_xusers
                WHERE userid = :username
                """;
        dicParam.addValue("username", username);
        Map<String, Object> perid = this.sqlRunner.getRow(sql, dicParam);
        String Perid = "";
        if(perid != null && perid.containsKey("perid")) {
            Perid = (String) perid.get("perid");
        }
        return Perid;
    }
    // xusers 정보 perid로 조회
    public Map<String, Object> getuserInfoPerid(String perid) {
        MapSqlParameterSource dicParam = new MapSqlParameterSource();

        String sql = """
                SELECT *
                FROM tb_xusers
                WHERE perid = :perid
                """;
        dicParam.addValue("perid", perid);
        Map<String, Object> userInfo = this.sqlRunner.getRow(sql, dicParam);
        return userInfo;
    }
    // 삭제 메서드
    public boolean delete(String username) {
        MapSqlParameterSource dicParam = new MapSqlParameterSource();

        String sql = """
               
                """;
        dicParam.addValue("username", username);
        Map<String, Object> perid = this.sqlRunner.getRow(sql, dicParam);
        String Perid = "";
        if(perid != null && perid.containsKey("perid")) {
            Perid = (String) perid.get("perid");
        }

        return true;
    }

}
