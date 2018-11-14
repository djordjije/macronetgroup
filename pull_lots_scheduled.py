import json, requests, time, logging, datetime


if __name__ == "__main__":
    logging.basicConfig(filename='pull_lots_scheduled.log', level=logging.INFO)
    now = datetime.datetime.now()
    headers = {'host': 'api.linnworks.net', 'connection': 'keep-alive', 'accept': 'application/json, text/javascript, */*; q=0.01',
                   'origin': 'https://linnworks.net', 'accept-language': 'en', 'user-agent': 'Chrome/42.0.2311.90',
                   'content-type': 'application/x-www-form-urlencoded; charset=UTF-8', 'referer': 'https://www.linnworks.net',
                   'accept-encoding': 'gzip, deflate'}
    payload = {'applicationId': '6c6edc81-5372-4309-bc72-aea610367d62', 'applicationSecret': 'b9c21192-f035-4d77-b967-4936cbcf5fc1',
               'token': 'ea37bff91d18d697849d045d31a4dd42'}

    r = requests.post('https://api.linnworks.net//api/Auth/AuthorizeByApplication', headers=headers, params=payload)

    json_loads = json.loads(json.dumps(r.json()))  # json_loads is type dict

    auth_token = json_loads["Token"]
    #logging.info("Passed Authorization Header.")

    ##########################################
    ###   AUTHORIZATION SECTION FINISHED   ###
    ##########################################

    # Get dictionary of lot SKUs and their IDs
    # IDs need to be sent to further calls to retrieve each SKU's info
    # IDs now exist in lot_id_dict
    sql_query = """
    SELECT si.ItemNumber, si.pkStockItemID
    FROM StockItem si
    WHERE si.ItemNumber LIKE 'LOT_%'
    AND si.ItemNumber NOT LIKE 'LOTOF%'
    AND si.bLogicalDelete = 0
    """
    r = requests.post('https://us-ext.linnworks.net//api/Dashboards/ExecuteCustomScriptQuery',
                  headers={'Authorization': auth_token}, params={'script': sql_query})

    lots = json.loads(r.content)
    lot_id_dict = {}
    logging.info(str(now) + ' ' + "Displaying the current LOT SKUs available in Linnworks")
    for item in lots["Results"]:
        lot_id_dict.update({item['ItemNumber']: item['pkStockItemID']})
    for key, value in lot_id_dict.items():
        print({key: value})
        logging.info(str(now) + ' ' + str({key: value}))


    # This pulls all of the lots data from LW POs. Sums the counts and totals of each.
    sql_query = """
    SELECT p.ExternalInvoiceNumber, SUM(pi.Quantity) AS 'Count', SUM(pi.Cost) AS 'Total',
    CASE
    WHEN p.ExternalInvoiceNumber LIKE '%-A' THEN LEFT(RIGHT(p.ExternalInvoiceNumber, 8), 6)
    ELSE RIGHT(p.ExternalInvoiceNumber, 6)
    END AS 'Date'
    FROM Purchase p
    LEFT OUTER JOIN PurchaseItem pi on p.pkPurchaseID = pi.fkPurchasId
    LEFT OUTER JOIN StockItem si on pi.fkStockItemId = si.pkStockItemID
    WHERE p.ExternalInvoiceNumber LIKE 'LOT_%' AND p.DateOfDelivery > DATEADD(hh, -6, GETDATE())
    GROUP BY p.ExternalInvoiceNumber ORDER BY Date DESC
    """

    r = requests.post('https://us-ext.linnworks.net//api/Dashboards/ExecuteCustomScriptQuery',
                  headers={'Authorization': auth_token}, params={'script': sql_query})

    data = json.loads(r.content)
    counts = {}
    totals = {}

    for lot in data["Results"]:
        invoice, count, total = (lot["ExternalInvoiceNumber"], int(lot["Count"]), float(lot["Total"]))
        invoice_num = invoice.split("-")
        invoice_num = str(invoice_num[0]).strip()
        if invoice_num in counts:
            count += int(counts[invoice_num])
            counts.update({invoice_num: count})
            total += float(totals[invoice_num])
            totals.update({invoice_num: round(total, 2)})
        else:
            counts.update({invoice_num: count})
            totals.update({invoice_num: round(total, 2)})
        print(invoice_num + ' ' + str(count))
        logging.info(str(now) + ' ' + str(invoice_num) + ' ' + str(count))
    print(counts)
    logging.info(str(now) + ' ' + str(counts))
    print(totals)
    logging.info(str(now) + ' ' + str(totals))

    for key in counts:
        if key not in lot_id_dict:
            print(str(key) + ' does not exist as a SKU in Linnworks.')
            #logging.info(str(now) + ' ' + str(key) + ' does not exist as a SKU in Linnworks.')
            continue
        stock_item_id = lot_id_dict[key]
        r = requests.post('https://us-ext.linnworks.net//api/Stock/GetStockLevel',
                          headers={'Authorization': auth_token},
                          params={'stockItemId': stock_item_id,
                                  })
        print(r.content)
        logging.info(str(now) + ' ' + str(r.content))
        data = json.loads(r.content)
        default = data[0]
        current_stock_level, current_stock_value = default["StockLevel"], float(default["StockValue"])
        print('StockLevel: ' + str(default["StockLevel"]))
        logging.info(str(now) + ' ' + 'StockLevel: ' + str(default["StockLevel"]))
        print('StockValue: ' + str(default["StockValue"]))
        logging.info(str(now) + ' ' + 'StockValue: ' + str(default["StockValue"]))
        # Update Stock Level first
        r = requests.post('https://us-ext.linnworks.net//api/Inventory/UpdateInventoryItemLevels',
                          headers={'Authorization': auth_token},
                          params={'inventoryItemId': stock_item_id,
                                  'fieldName': 'StockLevel',
                                  'locationId': '00000000-0000-0000-0000-000000000000',
                                  'fieldValue': str(current_stock_level-counts[key])})
        print("Results of updating stock level:")
        logging.info(str(now) + ' ' + "Results of updating stock level:")
        print(r.content)
        logging.info(str(now) + ' ' + str(r.content))
        print(current_stock_level-counts[key])
        logging.info(str(now) + ' ' + str(current_stock_level-counts[key]))
        # Update Stock Value last
        time.sleep(2)
        r = requests.post('https://us-ext.linnworks.net//api/Inventory/UpdateInventoryItemLevels',
                          headers={'Authorization': auth_token},
                          params={'inventoryItemId': stock_item_id,
                                  'fieldName': 'StockValue',
                                  'locationId': '00000000-0000-0000-0000-000000000000',
                                  'fieldValue': str(current_stock_value - totals[key])})
        print("Results of updating stock value:")
        logging.info(str(now) + ' ' + "Results of updating stock value:")
        print(r.content)
        logging.info(str(now) + ' ' + str(r.content))
        print('New Stock Value: ' + str(current_stock_value - totals[key]))
        logging.info(str(now) + ' ' + str(current_stock_value - totals[key]))
