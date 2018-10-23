package  HomeTestFromRB;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;	

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;



public class JavaSparkSQLExample  {

	//--������� ����, ������� ����� ������
    public static String pathInFileJson = "src/InData/Sourse/";    
    public static String pathfileArchive = "src/InData/Archive/";   
    
    
  //--������� �����, � ������� ����� ������    
    public static String pathPargetRegistered = "src/InData/Registered.parquet";
    public static String pathPargetAppLoad = "src/InData/App_loaded.parquet";    	
	
  public static void main(String[] args) {
	  	  
    SparkSession spark = SparkSession
      .builder()
      .appName("Java Spark SQL data sources example")
      .master("local")
      .config("spark.some.config.option", "some-value")
      .getOrCreate();      
   
   SaveInJsonIntoParquetFile(spark); //����� ������ � ���������� ������
   AnalysisEvents(spark); // ����� �������

    spark.stop();
  }

  //-------------------------------------------------------------------------------------------------------------------------------------------------------
// ��������� ������ ������ �� ���������������� ����� Json,
// � ��������� ���������� ����� � ��� Parquet �������� ������������  
  private static void SaveInJsonIntoParquetFile (SparkSession spark) { 
	    
	//--������ JSON ���� 
	  try {
	    Dataset<Row> DatasetInJson =  spark.read().format("json").load(pathInFileJson);
	    
	//--������������� �������� ����� � ������  ��  ������    
	    Dataset<Row> DataSetJson = 
	    		DatasetInJson.withColumn("time", DatasetInJson.col("_t").cast(DataTypes.TimestampType))
	    				.withColumn("email", DatasetInJson.col("_p").cast(DataTypes.StringType))
	    				.withColumn("channel", DatasetInJson.col("channel").cast(DataTypes.StringType))
	    				.withColumn("device_type", DatasetInJson.col("device_type").cast(DataTypes.StringType))
	    				.withColumn("action_type", DatasetInJson.col("_n").cast(DataTypes.StringType))    				
	    				;    

	//------------������ �����, ���� ��� ��� ����----------------------------------------------------------------------------------------------------------
	   // File fileReg = new File(pathPargetRegistered);    
	   // recursiveDelete(fileReg);    

	    //File fileApp = new File(pathPargetAppLoad);   	
	    //recursiveDelete(fileApp);   
	    
	//----------------------------------------------------------------------------------------------------------------------    
	//--�������� ������ � Parquet ������  - �����������
	    			DataSetJson
	    			.select("time", "email", "channel")    		
	      		    .where("action_type = \"registered\"")    	
	    			.write()    		    
	    			.mode(SaveMode.Append)  // -- ���������, ��� ���� ��������� ����� ������ � parquet FILE, ���� �� �������, �� �� �������, ��� ���� ����� ����
	    			.parquet(pathPargetRegistered)
	    			;
	    		   
	//--�������� ������ � Parquet ������  - �������� ����������
	    			DataSetJson
	    			.select("time", "email", "device_type")    				
	      		    .where("action_type = \"app_loaded\"")   
	    			.write()    	
	    			.mode(SaveMode.Append)	 // -- ���������, ��� ���� ��������� ����� ������ � parquet FILE, ���� �� �������, �� �� �������, ��� ���� ����� ����    			
	    			.parquet(pathPargetAppLoad);	    			
	    			; 
	    			
   // �������� ������������ ����� � �����	    			
      CopyJsonFileToArchive(); 	    			
      
	  }
	    catch(Exception ex){
	         
	        System.out.println("!!!��� ������ ��� ����������, ���� ��������� �� ������!!!! " + ex.getMessage());

	    }	    		   
	  	  
  }
  
//---------------------------------------------------------------------------------------------  
//�������� ������ ��� ������������ ������� app_loaded � registered   
//---------------------------------------------------------------------------------------------  
  private static void AnalysisEvents(SparkSession spark) {
     
    		   
//---������ ���� � �������
 try {	  
  Dataset<Row> parquetFileReg = spark.read().parquet(pathPargetRegistered);    
    
  //�������� �������, ������� ����� ������������ � SQL
  parquetFileReg.createOrReplaceTempView("parquetFileReg");

  //�������� ������� View_reg_cnt_all , ������� ���������� ���-�� ���������� email
  Dataset<Row> CntRegDS = spark.sql("SELECT count( distinct email ) as CNT , 1 as pk_id FROM parquetFileReg");
  CntRegDS.createOrReplaceTempView("View_reg_cnt_all");
    

  //-- �������, � ������� 1-� ���� email ������������, � 2-� ���� �����������
  Dataset<Row> DataSet�Reg = spark.sql("SELECT distinct email, CAST(time AS DATE) AS date_reg FROM parquetFileReg");  
  DataSet�Reg.createOrReplaceTempView("View_reg");
 }
 catch(Exception ex){
     
     System.out.println("!!! ���� � ������� �� ������������ ��� �� ������ !!!  " + ex.getMessage());
     return;  // -- ��������, ��� ��� ����������� �������� ������ ��� ������
 }	  
  
//---999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999 
//---������ ���� � �������
 try {	 
  Dataset<Row> parquetFileApp = spark.read().parquet(pathPargetAppLoad);    

//�������� �������, ������� ����� ������������ � SQL
  parquetFileApp.createOrReplaceTempView("View_app_load");

 }
 catch(Exception ex){
     
     System.out.println("!!! ���� � ������� �� ��������� ��� �� ������ !!!  " + ex.getMessage());
     return;  // -- ��������, ��� ��� ����������� �������� ������ ��� ������
 }	

// ���������� ���������� ����������� �������������, ������� � ������� ����������� ������ (����������� ������ ���������� � �����������, � ������������� � �����������), 
  // ��������� ����� �����������, ��������� �������� ���������� � ������ ���������� ������������������ �������������. 
  Dataset<Row> DataSetResult = spark.sql(" "
		+ "select round((v1.cnt/v2.cnt)*100,2) from ("
		+ "				SELECT count(distinct View_app_load.email)  as CNT, 1 as pk_id FROM  View_app_load ,  View_reg  where View_reg.email = View_app_load.email and  CAST(View_app_load.time AS DATE) between  next_day( View_reg.date_reg ,\"Mon\") and next_day(View_reg.date_reg,\"Sun\")"
		+ "				) v1, View_reg_cnt_all v2 "
		+ "where v1.pk_id = v2.pk_id");

//��������� ������� ������ ��������� � �������.
Dataset<String> DataSetResultRp = DataSetResult.map(
(MapFunction<Row, String>) row -> "Result = " + row.getDouble(0) + "%",
Encoders.STRING());

DataSetResultRp.show();
     

  }
//---------------------------------------------------------------------------------------------  
//  ����������� ������� �����, ���������� ��� �������� parquet ������
//---------------------------------------------------------------------------------------------    
  public static void recursiveDelete(File file) {
      // �� ����� ������������ �����
      if (!file.exists())
          return;
       
      //���� ��� �����, �� ���� ������ ���� ����� � �������� ����������� �������� �����, ��� ��� ����
      if (file.isDirectory()) {
          for (File f : file.listFiles()) {
              // ����������� �����
              recursiveDelete(f);
          }
      }
      // �������� ����� delete() ��� �������� ������ � ������(!) �����
      file.delete();
  }  

  
//---------------------------------------------------------------------------------------------  
//���������� ������������� ������ � �������-�����
//---------------------------------------------------------------------------------------------    
public static void CopyJsonFileToArchive() {

	File f = new File(pathfileArchive);
	try{
	    if(f.mkdir()) { 
	        System.out.println("Directory Created");
	    } else {
	        System.out.println("Directory is not created");
	    }
	  //��������� ����� � �����  
	    copyFileJsonDir(pathInFileJson, pathfileArchive );
	    
	  // ������� ����������� ����� �� ��������� ��������
	    deleteAllFilesFolder(pathInFileJson);
	    
	} catch(Exception e){
	    e.printStackTrace();
	} 
}  
//---------------------------------------------------------------------------------------------  
// �������� ����� � �������������� ������, ������� ������ ����������� ������� �����
//---------------------------------------------------------------------------------------------    
private static void copyFileJsonDir(String sourceDirName, String targetSourceDir) throws IOException {
    File folder = new File(sourceDirName);

    File[] listOfFiles = folder.listFiles();

    Path destDir = Paths.get(targetSourceDir);
    
    if (listOfFiles != null) 
    	System.out.println("�������� ������������ ����� � �����:"); 		
        for (File file : listOfFiles)
        {
            Files.copy(file.toPath(), destDir.resolve(file.getName()), StandardCopyOption.REPLACE_EXISTING);
            System.out.println(file.getName());
        }    

}
//---------------------------------------------------------------------------------------------  
// ������� ����� �� �����
//--------------------------------------------------------------------------------------------- 
public static void deleteAllFilesFolder(String path) 
	{
	 for (File myFile : new File(path).listFiles())
	        if (myFile.isFile()) myFile.delete();
}

}






