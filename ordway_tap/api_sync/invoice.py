from ordway_tap.api_sync.api import get_index_data
from ordway_tap.api_sync.utils import print_record
from ordway_tap.record.invoice import map_invoice_response


def sync():
    for invoice_response in get_index_data('/api/v1/invoices'):
        for invoice in map_invoice_response(invoice_response):
            print_record('invoices', invoice)
