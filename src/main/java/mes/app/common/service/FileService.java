package mes.app.common.service;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import mes.domain.entity.AttachFile;
import mes.domain.services.SqlRunner;

@Service
public class FileService {
	
	@Autowired
	SqlRunner sqlRunner;
	
	public List<Map<String, Object>> getAttachFile(String TableName , Integer DataPk, String attach_name){
		MapSqlParameterSource dicParam = new MapSqlParameterSource();        
        dicParam.addValue("TableName", TableName);
        dicParam.addValue("DataPk", DataPk);
        dicParam.addValue("attach_name", attach_name);
        //dicParam.addValue("limit", limit);
        
        String sql = """
					select id
		         , "TableName"
		         , "DataPk"
		         , "AttachName"
		         , "FileIndex"
		         , "FileName"
		         , "PhysicFileName"
		         , "ExtName"
		         , "FilePath"
		         , "_created"
		         , "ExtName" as "fileExt"
		         , "FileName" as "fileNm"
		         , "FileSize" as "fileSize"
		         , id as "fileId"
		         from attach_file 
		         where 1 = 1 
		         and "TableName" = :TableName
		         and "DataPk" = :DataPk
		         and "AttachName" = :attach_name
		         order by id desc
    		    """;
        
        //if (limit != null) sql += "limit :limit";
        
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);
		return items;
	}
	
	
	public Map<String, Object> getAttachFileDetail(Integer file_id){
		
		MapSqlParameterSource dicParam = new MapSqlParameterSource();        
        dicParam.addValue("file_id", file_id);
		
		String sql = """
				select id
			    , "TableName"
			    , "DataPk"
			    , "AttachName"
			    , "FileIndex"
			    , "FileName"
			    , "PhysicFileName"
			    , "ExtName"
			    , "FilePath"
			    , "_created"
			    , "FileSize"
			    from attach_file 
			    where id = :file_id
				""";
		
		Map<String, Object> item = this.sqlRunner.getRow(sql, dicParam);
		return item;
	}
	
	
	public int updateDataPk(Integer fileId, Integer DataPk){
		
		MapSqlParameterSource dicParam = new MapSqlParameterSource();        
        dicParam.addValue("fileId", fileId);
        dicParam.addValue("DataPk", DataPk);
        
        String sql = """
					update attach_file
					set "DataPk" = :DataPk
					where id in ( :fileId )
					""";
        
        int item = this.sqlRunner.execute(sql, dicParam);

		return item;

	}


	public void deleteByDataPk(int dataPk) {
		MapSqlParameterSource dicParam = new MapSqlParameterSource();        
        dicParam.addValue("DataPk", dataPk);
		
        String sql = """
					delete from attach_file
					where "DataPk"= :DataPk
					""";
        
        this.sqlRunner.execute(sql, dicParam);
	}
	

	public AttachFile getAttachFileByData(String tableName, Integer dataPk, String attachName) {
		AttachFile attFile = null;
		
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
		
		attFile = this.sqlRunner.queryForObject(sql, paramMap, new RowMapper<AttachFile>() {
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
		return attFile;
	}
		

}
