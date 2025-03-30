import datetime
import logging
import azure.functions as func
from .scraper import run_scraper

def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()
    logging.info('Python timer trigger function started at %s', utc_timestamp)
    
    if mytimer.past_due:
        logging.info('The timer is past due!')

    try:
        logging.info('Starting car scraper...')
        run_scraper()
        logging.info('Car scraper completed successfully')
    except Exception as e:
        logging.error(f'Error in car scraper: {str(e)}')
        import traceback
        logging.error(traceback.format_exc())

