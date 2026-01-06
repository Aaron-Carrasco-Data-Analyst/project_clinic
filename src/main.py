from src.extract import extract_data
from src.transform import transform_data
from src.extract import load_data

def main():
    print("🚀 Iniciando ETL...")

    # Extract
    df_raw = extract_data()
    print(f"Datos extraídos: {len(df_raw)} registros")

    # Transform
    df_clean = transform_data(df_raw)
    print(f"Datos transformados: {len(df_clean)} registros")

    # Load
    load_data(df_clean)
    print("✅ ETL finalizado con éxito")

if __name__ == "__main__":
    main()