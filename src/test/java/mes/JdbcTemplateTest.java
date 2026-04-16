package mes;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import mes.domain.entity.AttachFile;

@SpringBootTest
public class JdbcTemplateTest {

	
	
	
	@Autowired
    private NamedParameterJdbcTemplate  jdbcTemplate;
	
	@Test
	public void namedParameterTest() {
		
		String sql = "select * from bom where id=:id";
		
		Map<String, Object> paramMap = new HashMap<String, Object>();
		paramMap.put("id", 1);
		
		Map<String, Object> data = this.jdbcTemplate.queryForMap(sql, paramMap);
		System.out.println(data);
		
	}
	
	@Test
	public void selectNullTest() {
		
		String sql = """
				select bom.b_level as _level
                , m."Name" as mat_name
                , bom.bom_ratio
                , concat(bom.quantity,'->',bom.produced_qty) as bom_qty
                , fn_code_name('mat_type',mg."MaterialType") as mat_type
                , u."Name" as unit
                , m."Code" as mat_code
                , bom.prod_pk as my_key
                , bom.parent_prod_pk as parent_key
	            from tbl_bom_reverse(:mat_pk, to_char(now(),'yyyy-mm-dd')) as bom
                inner join material m on m.id = bom.prod_pk
                left join mat_grp mg on mg.id = m."MaterialGroup_id"
                left join unit u on u.id = m."Unit_id"
	            order by bom.tot_order
				""";
		
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("mat_pk", 126);
		
		List<Map<String, Object>> rows = null;
		//Map<String, Object> row = null;
    	
    	try {
    		rows = this.jdbcTemplate.queryForList(sql, dicParam);
    		//row = this.jdbcTemplate.queryForMap(sql, dicParam);
    		System.out.println("rows : "+rows.toString());
    		System.out.println("rows type : "+rows.getClass().getSimpleName());
		}
    	catch(DataAccessException de) {
    		System.out.println("DataAccessException : "+de.toString());
    	}
    	catch (Exception e) {
			// TODO: handle exception
    		System.out.println("Excetpion : "+e.toString());
		}
	}
	
	@Test
	public void mapperTest() {
	
		AttachFile attFile = null;
		
		String tableName = "doc_result";
		Integer dataPk =12;
		String attachName = "basic";
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("tableName", tableName);
		paramMap.addValue("dataPk", dataPk);
		paramMap.addValue("attachName", attachName);
		

		String sql = """
		select 
		id
		, "TableName"
		, "DataPk"
		, "AttachName"
		, "FileIndex"
		, "FileName"
		, "PhysicFileName"
		, "ExtName"
		,"FilePath"
		, "FileSize"
		, "_created" , "_modified" , "_creater_id" , "_modifier_id" 
		from attach_file af 
		where
		"TableName"=:tableName and "DataPk"=:dataPk and "AttachName"=:attachName
		""";
		
		attFile = this.jdbcTemplate.queryForObject(sql, paramMap, new RowMapper<AttachFile>() {
			@Override
			public AttachFile mapRow(ResultSet rs, int rowNum) throws SQLException {
				
				AttachFile att = new AttachFile();
				att.setId(rs.getInt("id"));
				att.setTableName(rs.getString("TableName"));
				att.setDataPk(rs.getInt("DataPk"));
				att.setAttachName(rs.getString("AttachName"));
				att.setFileIndex(rs.getInt("FileIndex"));
				att.setAttachName(rs.getString("FileName"));
				att.setPhysicFileName(rs.getString("PhysicFileName"));
				att.setExtName(rs.getString("ExtName"));
				att.setFilePath(rs.getString("FilePath"));
				att.setFileSize(rs.getInt("FileSize"));
				
				att.set_created(rs.getTimestamp("_created"));				
				att.set_modified(rs.getTimestamp("_modified"));				
				att.set_creater_id(rs.getInt("_creater_id"));
				att.set_modifier_id(rs.getInt("_modifier_id"));

				return att;
			}
		});		
		
		Assert.assertTrue(attFile!=null);		
	}	
	
}
