import boto3,csv, datetime
org_client = boto3.client('organizations')
ce_client = boto3.client('ce')

def get_CreditsCostQauntityUsage_ForService(account_id, service, sDate, eDate, granularity):
    
    credit_filter= {
        'And': [
            {
                'Dimensions': {
                    'Key': 'LINKED_ACCOUNT',
                    'Values': [account_id]
                }
            },
            {
                'Dimensions': {
                    'Key': 'SERVICE',
                    'Values': [service]
                },
            },
            {
                'Dimensions': {
                    'Key': 'RECORD_TYPE',
                    'Values': ['Credit']
                }
            }
        ]
    }
    
    cost_quantity_filter = {
        'And': [
            {
                'Dimensions': {
                    'Key': 'LINKED_ACCOUNT',
                    'Values': [account_id]
                }
            },
            {
                'Dimensions': {
                    'Key': 'SERVICE',
                    'Values': [service]
                },
            },
        ]
    }
    result = {'cost_quantity_usage':[], 'credit_usage':[]}
    
    for filter_params in [cost_quantity_filter, credit_filter]:
        response = ce_client.get_cost_and_usage(
            TimePeriod={
                'Start': sDate,
                'End': eDate
            },
            Granularity=granularity,
            Filter=filter_params,
            GroupBy=[
                {
                    'Type': 'DIMENSION',
                    'Key': 'USAGE_TYPE',
                },
            ],
            Metrics=["UnblendedCost", 'UsageQuantity']
        )
        if filter_params == cost_quantity_filter:
            result['cost_quantity_usage'].extend(response['ResultsByTime'][0]['Groups']) 
        elif filter_params == credit_filter:
            result['credit_usage'].extend(response['ResultsByTime'][0]['Groups']) 
    
    result_list = []
    credit_usage_mapping = {cu['Keys'][0]: cu['Metrics']['UnblendedCost']['Amount'] for cu in result['credit_usage']}
    
    for item in result['cost_quantity_usage']:
        usage_key = item['Keys'][0]
        credits = credit_usage_mapping.get(usage_key, '0')
        result_list.append({
            'Usage': usage_key,
            'UsageQuantity': item['Metrics']['UsageQuantity']['Amount'],
            'UnblendedCost': item['Metrics']['UnblendedCost'],
            'Credits': credits
        })
    return result_list

def get_cost_for_specific_service(account_id, service, sDate, eDate, granularity):
    
    response = ce_client.get_cost_and_usage(
        TimePeriod={
            'Start': sDate,
            'End': eDate
        },
        Granularity=granularity,
        Filter={
            'And': [
                {
                    'Dimensions': {
                        'Key': 'LINKED_ACCOUNT',
                        'Values': [account_id]
                    }
                },
                {
                    'Dimensions': {  
                        'Key': 'SERVICE',
                        'Values': [service]
                    },
                }]
        },
            GroupBy=[
            {
                'Type': 'DIMENSION',
                'Key': 'USAGE_TYPE',
            },
        ],
        Metrics=["UnblendedCost", "UsageQuantity"]
    )
    return response

def get_total_cost_for_services(account_id, sDate, eDate, granularity):
    response = ce_client.get_cost_and_usage(
        TimePeriod={
            'Start': sDate,
            'End': eDate
        },
        Granularity=granularity,
        Filter={
            'Dimensions': {
                        'Key': 'LINKED_ACCOUNT',
                        'Values': [account_id]
                    }
        },
            GroupBy=[
            {
                'Type': 'DIMENSION',
                'Key': 'SERVICE',
            },
        ],
        Metrics=["UnblendedCost"]
    )
    return response

def retrieve_all_accountOfou(id):
    result = {'OU':[], 'ACCOUNT':[]}
    ou_response = org_client.list_children(
            ParentId=id,
            ChildType='ORGANIZATIONAL_UNIT'
        )
    account_response = org_client.list_children(
        ParentId=id,
        ChildType='ACCOUNT'
    )
    result['OU'].extend(ou_response['Children'])
    result['ACCOUNT'].extend(account_response['Children'])

    for i in ou_response['Children']:
        sub_result = retrieve_all_accountOfou(i['Id'])
        result['OU'].extend(sub_result['OU'])
        result['ACCOUNT'].extend(sub_result['ACCOUNT'])
    return result

def retrieve_all_accounts(id):
    organizationUnit_response = org_client.list_organizational_units_for_parent(
        ParentId = id,
    )
    accounts = []
    root_response = org_client.describe_organization()
    accounts.append({'Id':root_response['Organization']['MasterAccountId'], 'Type':'ACCOUNT'})
    ous = organizationUnit_response['OrganizationalUnits']
    for ou in ous:
        ou_id = ou['Id']
        accounts += retrieve_all_accountOfou(ou_id)['ACCOUNT']
    
    account_ids_names = []
    for account in accounts:
        account_resp = org_client.describe_account(AccountId=account['Id'])
        detail = account_resp['Account']
        account_name = detail['Name']
        account_id = detail['Id']
        account_ids_names.append({'Id':account_id, 'ACCOUNT':account_name})
    return account_ids_names

def save_usages_cost_data_to_csv(sDate, eDate, granularity):

    cost_data = []
    total_AccData = []
    account_ids_names = retrieve_all_accounts('r-mi1i')

    for name_id in account_ids_names:

        response = get_total_cost_for_services(name_id['Id'], sDate, eDate, granularity)
        
        Total_UnblendedCost = 0
        Total_Credit = 0
        Total_tax = 0

        services = [i['Keys'][0] for i in response['ResultsByTime'][0]['Groups']]

        tax_usages = get_cost_for_specific_service(name_id['Id'], 'Tax',sDate, eDate, granularity)
        tax_usages_costs = [{'usage': group['Keys'][0], 'UnblendedCost': group['Metrics']['UnblendedCost']['Amount']} for group in tax_usages['ResultsByTime'][0]['Groups']]
          
        for service in services:
            if service != 'Tax':
                resp = get_CreditsCostQauntityUsage_ForService(name_id['Id'], service, sDate, eDate, granularity)
                
                for data in resp:
                    _usage = data['Usage']
                    UnblendedCost = data['UnblendedCost']['Amount']
                    UsageQuantity = data['UsageQuantity']
                    CurrencyCode = data['UnblendedCost']['Unit']
                    Credit = data['Credits']
                    tax = 0
                    for i in tax_usages_costs:
                        if _usage == i['usage']:
                            tax += float(i['UnblendedCost'])
                        elif 'NoUsageType' == i['usage']:
                            Total_tax = float(i['UnblendedCost'])
                        else:
                            tax 
                    Total_UnblendedCost += float(UnblendedCost)
                    Total_Credit += float(Credit)
                    Total_tax += tax

                    total_cost = str(float(UnblendedCost)+ tax+float(Credit))
                    if service == 'EC2 - Other' or service == 'Amazon Elastic Compute Cloud - Compute':
                        if ('In-Bytes' in _usage  or 'Out-Bytes' in _usage or 'Regional-Bytes' in _usage):
                            cost_data.append([name_id['Id'], name_id['ACCOUNT'], sDate, eDate, 'AWS Data Transfer', _usage, UsageQuantity, CurrencyCode, UnblendedCost, Credit, str(tax), total_cost])
                        else:
                            cost_data.append([name_id['Id'], name_id['ACCOUNT'], sDate, eDate, 'Amazon Elastic Compute Cloud', _usage, UsageQuantity, CurrencyCode, UnblendedCost, Credit, str(tax), total_cost])
                    else:
                        if ('In-Bytes' in _usage  or 'Out-Bytes' in _usage or 'Regional-Bytes' in _usage):
                            pass
                        else:
                            cost_data.append([name_id['Id'], name_id['ACCOUNT'], sDate, eDate, service, _usage, UsageQuantity, CurrencyCode, UnblendedCost, Credit, str(tax), total_cost])                                                                          
            else:
                pass
        Total_ServiceCost = Total_UnblendedCost + Total_Credit + Total_tax
        total_AccData.append(['', '', sDate, eDate, '', '', f"Total for linked account# {name_id['Id']} ({name_id['ACCOUNT']})", '', str(Total_UnblendedCost), str(Total_Credit), str(Total_tax), str(Total_ServiceCost)])  
                
    y, m, d = sDate.split('-')
    get_month = datetime.datetime(int(y), int(m), int(d)).strftime("%B")            
    csv_file_name = f"AWS_Billing_Data_{get_month}_{y}.csv"

    with open(csv_file_name, 'w', newline='') as csvfile:
        csv_writer = csv.writer(csvfile)
        csv_writer.writerow(['LinkedAccountId', 'LinkedAccountName', 'BillingPeriodStartDate', 'BillingPeriodEndDate', 'Service', 'UsageType', 'UsageQuantity', 'CurrencyCode', 'CostBeforeTax', 'Credits', 'Tax', 'TotalCost'])
        csv_writer.writerows(cost_data)
        csv_writer.writerows(total_AccData)
    print('file downloaded')
    
save_usages_cost_data_to_csv('2023-08-01', '2023-09-01', 'MONTHLY')



# def lambda_handler(event, context):

#     json_response = json.dumps(ou_accounts, default=str)
#     json_without_slash = json.loads(json_response)

#     return {
#         'statusCode' : 200,
#         'body' : json_without_slash
#     }