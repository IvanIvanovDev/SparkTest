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

	//--Зададим файл, который будем читать
    public static String pathInFileJson = "src/InData/Sourse/";    
    public static String pathfileArchive = "src/InData/Archive/";   
    
    
  //--Зададим файлы, в которые будем писать    
    public static String pathPargetRegistered = "src/InData/Registered.parquet";
    public static String pathPargetAppLoad = "src/InData/App_loaded.parquet";    	
	
  public static void main(String[] args) {
	  	  
    SparkSession spark = SparkSession
      .builder()
      .appName("Java Spark SQL data sources example")
      .master("local")
      .config("spark.some.config.option", "some-value")
      .getOrCreate();      
   
   SaveInJsonIntoParquetFile(spark); //Режим чтения и сохраниеен данных
   AnalysisEvents(spark); // Режим Анализа

    spark.stop();
  }

  //-------------------------------------------------------------------------------------------------------------------------------------------------------
// Выполянем чтение данных из предоставленного файла Json,
// И сохраянем полученные даные в два Parquet согласно специцикации  
  private static void SaveInJsonIntoParquetFile (SparkSession spark) { 
	    
	//--Читаем JSON файл 
	  try {
	    Dataset<Row> DatasetInJson =  spark.read().format("json").load(pathInFileJson);
	    
	//--Переопределим названия полей и задаим  им  формат    
	    Dataset<Row> DataSetJson = 
	    		DatasetInJson.withColumn("time", DatasetInJson.col("_t").cast(DataTypes.TimestampType))
	    				.withColumn("email", DatasetInJson.col("_p").cast(DataTypes.StringType))
	    				.withColumn("channel", DatasetInJson.col("channel").cast(DataTypes.StringType))
	    				.withColumn("device_type", DatasetInJson.col("device_type").cast(DataTypes.StringType))
	    				.withColumn("action_type", DatasetInJson.col("_n").cast(DataTypes.StringType))    				
	    				;    

	//------------Удалим файлы, если они уже есть----------------------------------------------------------------------------------------------------------
	   // File fileReg = new File(pathPargetRegistered);    
	   // recursiveDelete(fileReg);    

	    //File fileApp = new File(pathPargetAppLoad);   	
	    //recursiveDelete(fileApp);   
	    
	//----------------------------------------------------------------------------------------------------------------------    
	//--Сохраним данные в Parquet формат  - Регистрации
	    			DataSetJson
	    			.select("time", "email", "channel")    		
	      		    .where("action_type = \"registered\"")    	
	    			.write()    		    
	    			.mode(SaveMode.Append)  // -- Указываем, что надо добавлять новые записи в parquet FILE, если не указать, то он считает, что надо новый файл
	    			.parquet(pathPargetRegistered)
	    			;
	    		   
	//--Сохраним данные в Parquet формат  - Загрузка приложений
	    			DataSetJson
	    			.select("time", "email", "device_type")    				
	      		    .where("action_type = \"app_loaded\"")   
	    			.write()    	
	    			.mode(SaveMode.Append)	 // -- Указываем, что надо добавлять новые записи в parquet FILE, если не указать, то он считает, что надо новый файл    			
	    			.parquet(pathPargetAppLoad);	    			
	    			; 
	    			
   // Сохраним обработанные файлы в архив	    			
      CopyJsonFileToArchive(); 	    			
      
	  }
	    catch(Exception ex){
	         
	        System.out.println("!!!Нет файлов для обратоткли, либо изменился их формат!!!! " + ex.getMessage());

	    }	    		   
	  	  
  }
  
//---------------------------------------------------------------------------------------------  
//Выполним Анализ уже обработанных событий app_loaded и registered   
//---------------------------------------------------------------------------------------------  
  private static void AnalysisEvents(SparkSession spark) {
     
    		   
//---Читаем файл с данными
 try {	  
  Dataset<Row> parquetFileReg = spark.read().parquet(pathPargetRegistered);    
    
  //Создадим витрину, которую можно использовать в SQL
  parquetFileReg.createOrReplaceTempView("parquetFileReg");

  //создадим витрину View_reg_cnt_all , которая возвращает кол-во уникальных email
  Dataset<Row> CntRegDS = spark.sql("SELECT count( distinct email ) as CNT , 1 as pk_id FROM parquetFileReg");
  CntRegDS.createOrReplaceTempView("View_reg_cnt_all");
    

  //-- Витрина, в которой 1-е поле email пользователя, а 2-е дата регастрации
  Dataset<Row> DataSetКReg = spark.sql("SELECT distinct email, CAST(time AS DATE) AS date_reg FROM parquetFileReg");  
  DataSetКReg.createOrReplaceTempView("View_reg");
 }
 catch(Exception ex){
     
     System.out.println("!!! Файл с данными по регистрациям еще не создан !!!  " + ex.getMessage());
     return;  // -- Закончим, так как расчитывать значение дальше нет смысла
 }	  
  
//---999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999999 
//---Читаем файл с данными
 try {	 
  Dataset<Row> parquetFileApp = spark.read().parquet(pathPargetAppLoad);    

//Создадим витрину, которую можно использовать в SQL
  parquetFileApp.createOrReplaceTempView("View_app_load");

 }
 catch(Exception ex){
     
     System.out.println("!!! Файл с данными по загрузкам еще не создан !!!  " + ex.getMessage());
     return;  // -- Закончим, так как расчитывать значение дальше нет смысла
 }	

// Рассчитаем процентное соотношение пользователей, которые в течение календарной недели (календарная неделя начинается в понедельник, а заканчивается в воскресенье), 
  // следующей после регистрации, выполняли загрузку приложения к общему количеству зарегистрированных пользователей. 
  Dataset<Row> DataSetResult = spark.sql(" "
		+ "select round((v1.cnt/v2.cnt)*100,2) from ("
		+ "				SELECT count(distinct View_app_load.email)  as CNT, 1 as pk_id FROM  View_app_load ,  View_reg  where View_reg.email = View_app_load.email and  CAST(View_app_load.time AS DATE) between  next_day( View_reg.date_reg ,\"Mon\") and next_day(View_reg.date_reg,\"Sun\")"
		+ "				) v1, View_reg_cnt_all v2 "
		+ "where v1.pk_id = v2.pk_id");

//Результат расчета должен выводится в консоль.
Dataset<String> DataSetResultRp = DataSetResult.map(
(MapFunction<Row, String>) row -> "Result = " + row.getDouble(0) + "%",
Encoders.STRING());

DataSetResultRp.show();
     

  }
//---------------------------------------------------------------------------------------------  
//  Рекурсивная очистка папки, используем для удаления parquet файлов
//---------------------------------------------------------------------------------------------    
  public static void recursiveDelete(File file) {
      // до конца рекурсивного цикла
      if (!file.exists())
          return;
       
      //если это папка, то идем внутрь этой папки и вызываем рекурсивное удаление всего, что там есть
      if (file.isDirectory()) {
          for (File f : file.listFiles()) {
              // рекурсивный вызов
              recursiveDelete(f);
          }
      }
      // вызываем метод delete() для удаления файлов и пустых(!) папок
      file.delete();
  }  

  
//---------------------------------------------------------------------------------------------  
//Переместим обработанныйе файлый в каталог-архив
//---------------------------------------------------------------------------------------------    
public static void CopyJsonFileToArchive() {

	File f = new File(pathfileArchive);
	try{
	    if(f.mkdir()) { 
	        System.out.println("Directory Created");
	    } else {
	        System.out.println("Directory is not created");
	    }
	  //Скопируем фалый в архив  
	    copyFileJsonDir(pathInFileJson, pathfileArchive );
	    
	  // Удаляем обработыные файлы из входящего каталога
	    deleteAllFilesFolder(pathInFileJson);
	    
	} catch(Exception e){
	    e.printStackTrace();
	} 
}  
//---------------------------------------------------------------------------------------------  
// Копируем фалый с использованием потока, хороший способ копирования больших фалов
//---------------------------------------------------------------------------------------------    
private static void copyFileJsonDir(String sourceDirName, String targetSourceDir) throws IOException {
    File folder = new File(sourceDirName);

    File[] listOfFiles = folder.listFiles();

    Path destDir = Paths.get(targetSourceDir);
    
    if (listOfFiles != null) 
    	System.out.println("Копируем обработанные файлы в архив:"); 		
        for (File file : listOfFiles)
        {
            Files.copy(file.toPath(), destDir.resolve(file.getName()), StandardCopyOption.REPLACE_EXISTING);
            System.out.println(file.getName());
        }    

}
//---------------------------------------------------------------------------------------------  
// Удаляем файлы из папки
//--------------------------------------------------------------------------------------------- 
public static void deleteAllFilesFolder(String path) 
	{
	 for (File myFile : new File(path).listFiles())
	        if (myFile.isFile()) myFile.delete();
}

}






