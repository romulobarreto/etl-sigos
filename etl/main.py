import argparse
import glob
import logging
import os
import time
from datetime import datetime
from logging.handlers import RotatingFileHandler
import schedule

from extraction.reports.general_report import download_general_report
from extraction.reports.return_report import download_return_report
from load.loader import init_database, load_df_to_postgres
from transformation.transformer import transformar_general, transformar_return


def setup_logging():
    os.makedirs("logs", exist_ok=True)
    log_filename = f"logs/etl_{datetime.now().strftime('%Y-%m-%d')}.log"
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    file_handler = RotatingFileHandler(
        log_filename,
        maxBytes=2_000_000,
        backupCount=5,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Evita adicionar handlers duplicados
    if not any(isinstance(h, RotatingFileHandler) for h in logger.handlers):
        logger.addHandler(file_handler)
    if not any(
        isinstance(h, logging.StreamHandler)
        and not isinstance(h, RotatingFileHandler)
        for h in logger.handlers
    ):
        logger.addHandler(console_handler)

    logging.info(f"Logging configurado. Arquivo base: {log_filename}")


def cleanup_files(report_type: str) -> None:
    """Remove arquivos baixados e processados após sucesso do ETL."""
    downloads_dir = os.path.join(os.getcwd(), "etl", "downloads")

    if report_type == "return":
        patterns = [os.path.join(downloads_dir, "retorno*.csv")]
    else:  # general
        patterns = [os.path.join(downloads_dir, "relatorio_prot_geral*.csv")]

    for pattern in patterns:
        for file_path in glob.glob(pattern):
            try:
                os.remove(file_path)
                logging.info(f"Removido: {file_path}")
            except Exception as e:
                logging.warning(f"Não conseguiu remover {file_path}: {e}")


def run_etl(report: str, mode: str, keep_files: bool = False) -> None:
    """Executa uma rodada completa de ETL para o report/mode informados."""
    logging.info(f"Iniciando ETL report={report} mode={mode}")

    try:
        init_database()

        if report == "general":
            download_general_report(mode=mode)
            logging.info("Extração GENERAL concluída")
            df = transformar_general(mode)
            logging.info("Transformação GENERAL concluída")
            load_df_to_postgres(
                df,
                tabela="general_reports",
                mode=mode,
                coluna_data_execucao="DATA_EXECUCAO",
            )
            logging.info("Load GENERAL concluído")
        else:  # return
            download_return_report(mode=mode)
            logging.info("Extração RETURN concluída")
            df = transformar_return(mode)
            logging.info("Transformação RETURN concluída")
            load_df_to_postgres(
                df,
                tabela="return_reports",
                mode=mode,
                coluna_data_execucao="DATA_EXECUCAO",
            )
            logging.info("Load RETURN concluído")

        if not keep_files:
            cleanup_files(report)
            logging.info("Limpeza de arquivos concluída")

        logging.info("ETL finalizado com sucesso")
    except Exception as e:
        logging.exception(f"Falha durante o ETL: {e}")
        # No scheduler, a gente não levanta a exceção pra não matar o loop
        raise


def run_incremental_cycle() -> None:
    """Roda um ciclo incremental: GENERAL -> RETURN."""
    logging.info("======== Iniciando ciclo incremental agendado ========")
    try:
        run_etl(report="general", mode="incremental", keep_files=False)
    except Exception:
        # Já foi logado dentro de run_etl
        logging.error("Erro ao executar GENERAL incremental no ciclo agendado")

    try:
        run_etl(report="return", mode="incremental", keep_files=False)
    except Exception:
        logging.error("Erro ao executar RETURN incremental no ciclo agendado")

    logging.info("======== Fim do ciclo incremental agendado ========")


def start_scheduler() -> None:
    """Configura e inicia o agendador: todo dia das 08:30 até 17:30, de hora em hora."""
    setup_logging()
    logging.info("Iniciando scheduler de ETL (general/return incrementais)")

    # horários: 08:30, 09:30, ..., 17:30
    horarios = [f"{h:02d}:30" for h in range(8, 18)]
    for h in horarios:
        schedule.every().day.at(h).do(run_incremental_cycle)
        logging.info(f"Ciclo incremental agendado para {h} diariamente")

    logging.info("Scheduler iniciado. Aguardando horários...")

    try:
        while True:
            schedule.run_pending()
            time.sleep(10)
    except KeyboardInterrupt:
        logging.info("Scheduler interrompido manualmente (Ctrl+C). Encerrando.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="ETL SIGOS")

    parser.add_argument(
        "--report",
        choices=["general", "return"],
        help="Tipo de relatório",
    )
    parser.add_argument(
        "--mode",
        choices=["full", "incremental"],
        help="Modo do relatório",
    )
    parser.add_argument(
        "--keep-files",
        action="store_true",
        help="Não deletar downloads/processed ao final",
    )
    parser.add_argument(
        "--scheduler",
        action="store_true",
        help="Inicia o modo agendador (ignora --report/--mode)",
    )

    args = parser.parse_args()

    # Validação: ou usa scheduler, ou usa report/mode
    if not args.scheduler:
        if not args.report or not args.mode:
            parser.error(
                "Você deve informar --report e --mode, ou então usar --scheduler.",
            )

    return args


def main() -> None:
    args = parse_args()

    if args.scheduler:
        # Modo agendador: não usa report/mode individuais
        start_scheduler()
    else:
        setup_logging()
        run_etl(
            report=args.report,
            mode=args.mode,
            keep_files=bool(args.keep_files),
        )


if __name__ == "__main__":
    main()