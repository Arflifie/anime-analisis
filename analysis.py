import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import scipy.stats as stats

def main():
    print("Inisialisasi Apache Spark Session...")
    # Inisialisasi Spark Session
    spark = SparkSession.builder \
        .appName("AnimeStudioPopularityAnalysis") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .getOrCreate()
    
    # Menonaktifkan log level INFO agar output lebih bersih
    spark.sparkContext.setLogLevel("WARN")

    dataset_path = "anime_dataset.csv"
    if not os.path.exists(dataset_path):
        print(f"Error: File '{dataset_path}' tidak ditemukan di direktori saat ini.")
        sys.exit(1)
        
    print(f"Membaca dataset dari {dataset_path}...")
    df = spark.read.csv(dataset_path, header=True, inferSchema=True)
    
    print("Melakukan Preprocessing Data...")
    # Filter data: Pastikan studio dan popularitas tidak null/Unknown
    df_cleaned = df.filter(
        col("studio").isNotNull() & 
        (col("studio") != "Unknown") & 
        col("popularity").isNotNull()
    )
    
    # Hitung jumlah anime per studio dan filter studio dengan minimal 10 judul anime
    print("Menyaring studio dengan minimal 10 judul anime untuk keabsahan statistik...")
    studio_counts = df_cleaned.groupBy("studio").count()
    popular_studios = studio_counts.filter(col("count") >= 10)
    
    # Gabungkan kembali untuk menyaring data utama
    df_final = df_cleaned.join(popular_studios, "studio").select("studio", "popularity")
    
    # Mengelompokkan data popularitas per studio
    print("Mengagregasi data popularitas berdasarkan studio...")
    grouped_data = df_final.groupBy("studio") \
        .agg({"popularity": "collect_list"}) \
        .collect()
        
    # Ekstrak list popularitas untuk tiap studio
    studio_groups = [row["collect_list(popularity)"] for row in grouped_data]
    studio_names = [row["studio"] for row in grouped_data]
    
    print(f"Total studio yang dianalisis: {len(studio_groups)}")
    
    # Menjalankan Uji Kruskal-Wallis menggunakan SciPy
    print("\nMenjalankan Uji Statistik Kruskal-Wallis...")
    # H0: Distribusi tingkat popularitas anime sama di semua studio (Tidak ada pengaruh).
    # H1: Setidaknya satu studio memiliki tingkat popularitas yang berbeda secara signifikan.
    
    stat, p_value = stats.kruskal(*studio_groups)
    
    print("\n" + "="*50)
    print(" HASIL ANALISIS STATISTIK KRUSKAL-WALLIS ".center(50, "="))
    print("="*50)
    print(f"Kruskal-Wallis H-Statistic : {stat:.4f}")
    print(f"P-Value                    : {p_value:.8e}")
    print("-"*50)
    
    alpha = 0.05
    if p_value < alpha:
        print("KESIMPULAN: TOLAK H0 (Hipotesis Nol)")
        print("Terdapat pengaruh yang SIGNIFIKAN dari Studio terhadap tingkat popularitas anime!")
        print("Reputasi, kualitas produksi, atau branding dari studio animasi tertentu terbukti")
        print("berdampak nyata pada seberapa populer anime tersebut di mata penonton.")
    else:
        print("KESIMPULAN: GAGAL TOLAK H0 (Hipotesis Nol)")
        print("TIDAK terdapat pengaruh yang signifikan dari Studio terhadap tingkat popularitas anime.")
        print("Popularitas anime kemungkinan lebih dipengaruhi oleh faktor lain seperti genre,")
        print("sumber cerita asli (manga/light novel), atau musim penayangan.")
    print("="*50)

    # Tutup Spark Session
    spark.stop()

if __name__ == "__main__":
    main()