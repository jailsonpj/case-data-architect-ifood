from authentication.authentication import AuthenticationS3
from extraction.extraction import ExtractionTLCData


x = ExtractionTLCData()

x.execute_extraction_process(colors=['green'], months=1)