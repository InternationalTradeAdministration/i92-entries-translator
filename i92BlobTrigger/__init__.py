import logging
import azure.functions as func
from .service import handler


def main(myblob: func.InputStream):
    if ".xls" in myblob.name:
        logging.info(f"Python blob trigger function processed blob \n"
                    f"Name: {myblob.name}\n"
                    f"Blob Size: {myblob.length} bytes")

        handler(myblob)