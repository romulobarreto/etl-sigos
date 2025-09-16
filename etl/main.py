import os
import glob
import argparse
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime

from extraction.reports.general_report import download_general_report
from extraction.reports.return_report import download_return_report
from transformation.transformer import transformar_general, transformar_return
from load.loader import init_database, load_df_to_postgres

def setup_logging():
    os.makedirs("logs", exist_ok=True)
    log_filename = f"logs/etl_{datetime.now().strftime('%Y-%m-%d')}.log"
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    
    file_handler = RotatingFileHandler(log_filename, maxBytes=2_000_000, backupCount=5, encoding="utf-8")
    file_handler.setFormatter(formatter)
    
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    if not any(isinstance(h, RotatingFileHandler) for h in logger.handlers):
        logger.addHandler(file_handler)
    if not any(isinstance(h, logging.StreamHandler) and not isinstance(h, RotatingFileHandler) for h in logger.handlers):
        logger.addHandler(console_handler)
    
    logging.info(f"Logging configurado. Arquivo base: {log_filename}")

def cleanup_files(report_type):
    """Remove arquivos baixados e processados após sucesso do ETL"""
    # Usa o mesmo caminho que o browser.py e transformer.py
    downloads_dir = os.path.join(os.getcwd(), "etl", "downloads")
    
    patterns = []
    if report_type == "return":
        patterns = [
            os.path.join(downloads_dir, "retorno*.csv")
        ]
    else:  # general
        patterns = [
            os.path.join(downloads_dir, "relatorio_prot_geral*.csv")
        ]
    
    for pattern in patterns:
        for file_path in glob.glob(pattern):
            try:
                os.remove(file_path)
                logging.info(f"Removido: {file_path}")
            except Exception as e:
                logging.warning(f"Não conseguiu remover {file_path}: {e}")

def main():
    parser = argparse.ArgumentParser(description="ETL SIGOS")
    parser.add_argument("--report", choices=["general", "return"], required=True, help="Tipo de relatório")
    parser.add_argument("--mode", choices=["full", "incremental"], required=True, help="Modo do relatório")
    parser.add_argument("--keep-files", action="store_true", help="Não deletar downloads/processed ao final")
    args = parser.parse_args()

    setup_logging()
    logging.info(f"Iniciando ETL report={args.report} mode={args.mode}")

    try:
        init_database()
        
        if args.report == "general":
            download_general_report(mode=args.mode)
            logging.info("Extração GENERAL concluída")
            df = transformar_general(args.mode)
            logging.info("Transformação GENERAL concluída")
            load_df_to_postgres(df, tabela="general_reports", mode=args.mode, coluna_data_execucao="DATA_EXECUCAO")
            logging.info("Load GENERAL concluído")
            
        else:  # return
            download_return_report(mode=args.mode)
            logging.info("Extração RETURN concluída")
            df = transformar_return(args.mode)
            logging.info("Transformação RETURN concluída")
            load_df_to_postgres(df, tabela="return_reports", mode=args.mode, coluna_data_execucao="DATA_EXECUCAO")
            logging.info("Load RETURN concluído")

        if not args.keep_files:
            cleanup_files(args.report)
            logging.info("Limpeza de arquivos concluída")

        logging.info("ETL finalizado com sucesso")
    except Exception as e:
        logging.exception(f"Falha durante o ETL: {e}")
        raise

if __name__ == "__main__":
    main()