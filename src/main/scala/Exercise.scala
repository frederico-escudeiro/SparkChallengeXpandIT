
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, LongType, StringType}
import scala.Double.NaN

object Exercise {
  def main(args : Array[String]): Unit = {
    import org.apache.spark.sql.SparkSession
    val spark = SparkSession.builder.master("local[*]").appName("SparkByExample").getOrCreate
    spark.sparkContext.setLogLevel("WARN")

    //googleplaystore.csv
    val fromcsv = spark.read
      .option("header","true") // 1a linha => header
      .option("quote", "\"") // tudo o que começa e acaba com " é quote
      .option("escape", "\"") // e se encontrar " dentro de quotes é suposto dar escape
      .option("mode","DROPMALFORMED")
      // Retirou o "Life Made WI-Fi Touchscreen Photo Frame" que tinha "Rating" de 19. Não tinha campo "Category" então ficou mal formatado
      .csv("src\\resources\\googleplaystore.csv")
      .cache

    //googleplaystore_reviews.csv
    val fromcsv_reviews = spark.read
      .option("header","true") // 1a linha => header
      .option("quote", "\"") // tudo o que começa e acaba com " é quote
      .option("escape", "\"") // e se encontrar " dentro de quotes é suposto dar escape
      .csv("src\\resources\\googleplaystore_user_reviews.csv")
      .cache


    /********************************************************** PART 1 ****************************************/

    val df_1 = fromcsv_reviews.select(
                col("App").cast(StringType),
                col("Sentiment_Polarity").cast(DoubleType))
      .withColumn("Sentiment_Polarity",
        when(col("Sentiment_Polarity").equalTo("nan"), NaN)
      .otherwise(col("Sentiment_Polarity"))) // Se existir "nan" então é transformado em NaN
      .na.fill(0.0) // Se existir NaN é posto um 0.0 (Double) no lugar dele.
      .groupBy("App") //Faz um grupo por App
      .agg(avg("Sentiment_Polarity").alias("Average_Sentiment_Polarity")) //Para cada App faz a média das Polaridades

    println("Exercise 1")
    df_1.show

    /********************************************************* PART 2 *****************************************/

    val df_2 = fromcsv.select(
      col("App").cast(StringType),col("Rating").cast(DoubleType))
      .na.fill(0.0) //NaN e null tornam-se 0.0
      .filter(col("Rating")>= 4.0) //Filtra pelos que têm Ratings >= 4.0
      .orderBy(desc("Rating")) //Mostra os maiores Ratings primeiro

    df_2
      .repartition(1) //Isto faz com que 1 worker esteja a processar o ficheiro todo sozinho
      .write.format("csv") //Escreve no formato csv
      .option("sep","§") //Em vez de "," o separador passa a ser "§"
      .save("src\\results\\best_apps.csv") //Guarda numa diretoria best_apps.csv
    // Tentei usar o DariaWriters (plug-in para spark) que consegue renomear o ficheiro mas não comprimia em "gzip"
    // entao discartei a hipótese

    /********************************************************* PART 3 *****************************************/

      /*
      Este foi o exercicio mais complicado para mim:
      A função agg que me permitia utilizar outras funções uteis não me permitia selecionar tabelas que não fossem por ela afetadas
      Assim usei um join para juntar a tabela que usava funções de agregação com a tabela que não as usava
      Tentei usar SQL puro mas senti que estava a passar à frente de algumas funções do spark
      Utilizei o orderedColumns para aplicar no fim a sequência pedida.
       */
  val orderedColumns: Array[String] = Array("App","Categories","Rating","Reviews","Size","Installs","Type","Price","Content_Rating","Genres","Last_Updated","Current_Version","Minimum_Android_Version")

    // Tabela com as funções de agregação
    val df_3_helper = fromcsv
      .groupBy(col("App").alias("AppNew")) //Agrupa por App (Dei o nome de AppNew para não haver ambiguidade de tabelas)
    .agg(
      // A função collect_set -> coloca as várias Categorias das Apps que apareciam duplicadas num Array
      collect_set(col("Category")).alias("Categories"), // que depois renomeei para Categories como pedido
      functions.max(col("Reviews"))) // Para cada grupo formado por cada App escolhe a linha com maior número de Reviews

   val df_3_helper1 = df_3_helper.join(fromcsv, fromcsv.col("App") === df_3_helper.col("AppNew"))

    // Alterações de Nomes
     // Foram retiradas as colunas App e Category da tabela fromcsv e assim apenas ficaram as AppNew e Categories da tabela
     // com as funções de agregação
     .drop("Category")
     .drop(col("App"))
     //Renomeei as colunas como era pretendido
     .withColumnRenamed("AppNew","App")
     .withColumnRenamed("Last Updated","Last_Updated")
     .withColumnRenamed("Current Ver","Current_Version")
     .withColumnRenamed("Android Ver","Minimum_Android_Version")
     .withColumnRenamed("Content Rating","Content_Rating")


     //Reviews -> cast para Long e tornar o 0 como "default"
     .withColumn("Reviews",col("Reviews").cast(LongType))
     .withColumn("Reviews",when(col("Reviews").isNull || col("Reviews").isNaN ,0L)
       .otherwise(col("Reviews"))) //Valor por defeito = 0 -> Se existir NaN ou Null é retirado.

     //Size -> cast para Double, tornar o null como "default" e retornar em MegaBytes
     .withColumn("Size",
       when(col("Size").contains("Varies with device"),null) //Varies with device -> null
         .otherwise(
           when(col("Size").contains("k"),col("Size").substr(lit(1), length(col("Size"))-1).cast(DoubleType)/1024)
             // (Numero)k -> Numero/1024 que é o MB
             .otherwise(
               when(col("Size").contains("M"),col("Size").substr(lit(1), length(col("Size"))-1).cast(DoubleType))
                 //(Numero)M -> Numero que é o MB
                 .otherwise(col("Size").cast(DoubleType))))) //Se for null continua null (último caso)

      //Price -> cast to Double, dollar to euro (euro = dollar*0.9)
     .withColumn("Price",
       when(col("Price").contains("$"),col("Price").substr(lit(2), length(col("Price"))).cast(DoubleType)*0.9)
       .otherwise(col("Price").cast(DoubleType)))

     // Genres -> split: string -> array[string] com divisão de ";"
     .withColumn("Genres",functions.split(col("Genres"),";"))
     //Colocar as tabelas com a ordem pretendida
     .select(orderedColumns.head, orderedColumns.tail: _*)

     .orderBy(desc("Reviews")) /*Coloca em primeiro lugar a linha com mais Reviews
      Retira as linhas com todas que têm tudo igual menos a coluna Categories e a coluna Reviews
     Ou seja, como as Reviews estão por ordem decrescente, a primeira linha mantida é a que tem
      maior número de Reviews para uma dada App, como pretendido   */
     .dropDuplicates("App","Rating","Size","Installs","Type","Price","Content_Rating","Genres","Current_Version","Minimum_Android_Version")



    val df_3_helper2 = df_3_helper1.withColumn("Reviews",when(col("Reviews").isNull || col("Reviews").isNaN ,0L)
      .otherwise(col("Reviews"))) //Valor por defeito = 0 -> Se existir NaN ou Null é retirado.


    val df_3 = df_3_helper2.withColumn("Size",
    when(col("Size").contains("Varies with device"),null) // "Varies with device" passa a ser null devido ao cast
      .otherwise(
        when(col("Size").contains("k"),col("Size").substr(lit(1), length(col("Size"))-1).cast(DoubleType)/1024)
          // (Numero)kB passa a ser Numero/1024 que é o MB
          .otherwise(
            when(col("Size").contains("M"),col("Size").substr(lit(1), length(col("Size"))-1).cast(DoubleType))
              //(Numero)MB passa a ser Numero que é o MB
              .otherwise(col("Size"))))) //Se for null continua null (último caso)

    println("Exercise 3")
    df_3.show()
    df_3.printSchema()

    /********************************************************* PART 4 *****************************************/

      // Para não haver ambiguidade de Nomes de Colunas (devido ao join):
    val df_1_helper = df_1.withColumnRenamed("App","AppNew")

    //Join dos DataFrames df_3 e df_1 que colocavam o Average_Sentiment_Polarity.
    val df_ex_4 = df_3.join(df_1_helper,df_1_helper.col("AppNew") === df_3.col("App"))
      //Retiro a AppNew para não have 2 tabelas App iguais
      .drop("AppNew")
      /*  Guardar como parquet e com compressão gzip.
          Fez-me confusão que o parquet vinha depois do gzip: "gz.parquet"
      */
      df_ex_4.repartition(1).write.option("compression","gzip").parquet("src\\results\\googleplaystore_cleaned")

    /********************************************************* PART 5 *****************************************/

      /* Este exercicio só demorou mais tempo porque estava a tentar perceber como juntar as tabelas df_1 e df_3,
         tendo em conta que depois iriam ficar agrupadas por género e isso podia de algum modo perturbar o Average_Sentiment_Polarity
         então o que eu fiz foi:
      */

      // Para não haver ambiguidade de Nomes de Colunas (devido ao join):
    val df_1_new = df_1.withColumnRenamed("App","AppNew")
    val df_4_helper = df_3.join(df_1_new,df_1_new.col("AppNew") === df_3.col("App"))
    //Usei a função explode que por cada argumento de um array cria uma nova linha e que foi muito útil para a coluna Genres
    val df_4 = df_4_helper.select(explode(col("Genres")).alias("Genre"),col("App"),col("Rating"),col("Average_Sentiment_Polarity"))
      .groupBy("Genre") //Agrupei por género
      .agg(count("App").alias("Count"), //Fiz o count do número de Apps por género
        avg("Rating").alias("Average_Rating"), //Calculei a média de Ratings por género

        // E por fim fiz a média de Sentiment polarities que estavam na df_1 (por App) e como um Género contém
        // várias Apps na minha cabeça bateu tudo certo.
        avg("Average_Sentiment_Polarity").alias("Average_Sentiment_Polarity")
      ).cache


    // E Guardei num ficheiro como anteriormente
    df_4.repartition(1).write.option("compression","gzip").parquet("src\\results\\googleplaystore_metrics")


    /*********************** Muito Obrigado pelo desafio! ****************************/


    spark.stop()
  }
}