from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim

def clean_anime_data(spark_session, file_path):
    """
    Membersihkan dan mempersiapkan dataset anime untuk analisis statistik.
    
    Parameters:
        spark_session (SparkSession): Objek session Apache Spark yang aktif.
        file_path (str): Path ke file anime_dataset.csv.
        
    Returns:
        DataFrame: Spark DataFrame yang telah dibersihkan dan siap dianalisis.
    """
    print(f"[Preprocessing] Membaca data dari: {file_path}")
    df = spark_session.read.csv(file_path, header=True, inferSchema=True)
    
    # 1. Bersihkan spasi kosong (whitespace) pada nama kolom jika ada
    for col_name in df.columns:
        df = df.withColumnRenamed(col_name, col_name.strip())
        
    # 2. Pembersihan baris dan kolom yang relevan
    print("[Preprocessing] Membersihkan kolom studio dan popularitas...")
    df_cleaned = df.filter(
        col("studio").isNotNull() & 
        (trim(col("studio")) != "") & 
        (col("studio") != "Unknown") & 
        col("popularity").isNotNull()
    )
    
    # Konversi kolom popularitas ke integer secara eksplisit
    df_cleaned = df_cleaned.withColumn("popularity", col("popularity").cast("integer"))
    
    return df_cleaned

if __name__ == "__main__":
    # Script test mandiri untuk preprocessing
    spark = SparkSession.builder \
        .appName("AnimePreprocessingTest") \
        .getOrCreate()
        
    try:
        cleaned_df = clean_anime_data(spark, "anime_dataset.csv")
        cleaned_df.show(5)
        print(f"Total baris setelah pembersihan: {cleaned_df.count()}")
    except Exception as e:
        print(f"Terjadi kesalahan saat preprocessing: {e}")
    finally:
        spark.stop()