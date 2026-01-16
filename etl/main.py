"""Módulo principal para execução e agendamento do ETL SIGOS."""

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
    """Configura o sistema de logging com rotação de arquivos e console."""
    os.makedirs('logs', exist_ok=True)
    log_filename = f"logs/etl_{datetime.now().strftime('%Y-%m-%d')}.log"
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    file_handler = RotatingFileHandler(
        log_filename,
        maxBytes=2_000_000,
        backupCount=5,
        encoding='utf-8',
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

    logging.info(f'Logging configurado. Arquivo base: {log_filename}')


def cleanup_files(report_type: str) -> None:
    """Remove arquivos baixados e processados após sucesso do ETL."""
    downloads_dir = os.path.join(os.getcwd(), 'etl', 'downloads')

    if report_type == 'return':
        patterns = [os.path.join(downloads_dir, 'retorno*.csv')]
    else:  # general
        patterns = [os.path.join(downloads_dir, 'relatorio_prot_geral*.csv')]

    for pattern in patterns:
        for file_path in glob.glob(pattern):
            try:
                os.remove(file_path)
                logging.info(f'Removido: {file_path}')
            except Exception as e:
                logging.warning(f'Não conseguiu remover {file_path}: {e}')


def run_etl(report: str, mode: str, keep_files: bool = False) -> None:
    """Executa uma rodada completa de ETL para o report/mode informados."""
    logging.info(f'Iniciando ETL report={report} mode={mode}')

    try:
        init_database()

        if report == 'general':
            download_general_report(mode=mode)
            logging.info('Extração GENERAL concluída')
            df = transformar_general(mode)
            logging.info('Transformação GENERAL concluída')
            load_df_to_postgres(
                df,
                tabela='general_reports',
                mode=mode,
                coluna_data_execucao='DATA_EXECUCAO',
            )
            logging.info('Load GENERAL concluído')
        else:  # return
            download_return_report(mode=mode)
            logging.info('Extração RETURN concluída')
            df = transformar_return(mode)
            logging.info('Transformação RETURN concluída')
            load_df_to_postgres(
                df,
                tabela='return_reports',
                mode=mode,
                coluna_data_execucao='DATA_EXECUCAO',
            )
            logging.info('Load RETURN concluído')

        if not keep_files:
            cleanup_files(report)
            logging.info('Limpeza de arquivos concluída')

        logging.info('ETL finalizado com sucesso')
    except Exception as e:
        logging.exception(f'Falha durante o ETL: {e}')
        raise


def run_incremental_cycle() -> None:
    """Roda um ciclo incremental: GENERAL -> RETURN."""
    logging.info('======== Iniciando ciclo incremental ========')
    try:
        run_etl(report='general', mode='incremental', keep_files=False)
    except Exception:
        logging.error('Erro ao executar GENERAL incremental no ciclo')

    try:
        run_etl(report='return', mode='incremental', keep_files=False)
    except Exception:
        logging.error('Erro ao executar RETURN incremental no ciclo')

    logging.info('======== Fim do ciclo incremental ========')


def run_full_cycle() -> None:
    """Roda um ciclo FULL: GENERAL -> RETURN."""
    logging.info('======== Iniciando ciclo FULL ========')
    try:
        run_etl(report='general', mode='full', keep_files=False)
    except Exception:
        logging.error('Erro ao executar GENERAL full no ciclo')

    try:
        run_etl(report='return', mode='full', keep_files=False)
    except Exception:
        logging.error('Erro ao executar RETURN full no ciclo')

    logging.info('======== Fim do ciclo FULL ========')


def start_scheduler() -> None:
    """
    Configura e inicia o agendador de tarefas.

    - Segunda a sábado: incrementais de hora em hora (08:30 até 17:30)
    - Domingo às 10:00: full (general + return)
    """
    setup_logging()
    logging.info('Iniciando scheduler de ETL')

    # Ciclo incremental: segunda a sábado, das 08:30 até 17:30
    horarios = [f'{h:02d}:30' for h in range(8, 18)]
    for h in horarios:
        schedule.every().monday.at(h).do(run_incremental_cycle)
        schedule.every().tuesday.at(h).do(run_incremental_cycle)
        schedule.every().wednesday.at(h).do(run_incremental_cycle)
        schedule.every().thursday.at(h).do(run_incremental_cycle)
        schedule.every().friday.at(h).do(run_incremental_cycle)
        schedule.every().saturday.at(h).do(run_incremental_cycle)

    logging.info(
        f"Ciclo incremental agendado para segunda a sábado nos horários: {', '.join(horarios)}"
    )

    # Ciclo FULL: domingo às 10:00
    schedule.every().sunday.at('10:00').do(run_full_cycle)
    logging.info('Ciclo FULL agendado para domingo às 10:00')

    logging.info('Scheduler iniciado. Aguardando horários...')

    try:
        while True:
            schedule.run_pending()
            time.sleep(10)
    except KeyboardInterrupt:
        logging.info(
            'Scheduler interrompido manualmente (Ctrl+C). Encerrando.'
        )


def parse_args() -> argparse.Namespace:
    """Realiza o parse dos argumentos da linha de comando."""
    parser = argparse.ArgumentParser(description='ETL SIGOS')

    parser.add_argument(
        '--report',
        choices=['general', 'return'],
        help='Tipo de relatório',
    )
    parser.add_argument(
        '--mode',
        choices=['full', 'incremental'],
        help='Modo do relatório',
    )
    parser.add_argument(
        '--keep-files',
        action='store_true',
        help='Não deletar downloads/processed ao final',
    )
    parser.add_argument(
        '--scheduler',
        action='store_true',
        help='Inicia o modo agendador (ignora --report/--mode)',
    )
    parser.add_argument(
        '--cycle-incremental',
        action='store_true',
        help='Executa o ciclo incremental completo (General + Return)',
    )
    parser.add_argument(
        '--cycle-full',
        action='store_true',
        help='Executa o ciclo FULL completo (General + Return)',
    )

    args = parser.parse_args()

    # Validação: precisa de pelo menos um modo de execução
    if not any([args.scheduler, args.cycle_incremental, args.cycle_full, args.report]):
        parser.error(
            'Você deve informar --report e --mode, --scheduler, --cycle-incremental ou --cycle-full.'
        )

    # Se usar report, precisa de mode
    if args.report and not args.mode:
        parser.error('Ao usar --report, você deve informar --mode também.')

    return args


def main() -> None:
    """Ponto de entrada principal da aplicação."""
    args = parse_args()

    if args.scheduler:
        start_scheduler()
    elif args.cycle_incremental:
        setup_logging()
        run_incremental_cycle()
    elif args.cycle_full:
        setup_logging()
        run_full_cycle()
    else:
        setup_logging()
        run_etl(
            report=args.report,
            mode=args.mode,
            keep_files=bool(args.keep_files),
        )


if __name__ == '__main__':
    main()