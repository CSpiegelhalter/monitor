import json, sys, os, requests
from SPARQLWrapper import SPARQLWrapper, JSON

print('imported all modules')

neptuneEndpoint = os.environ['neptuneEndpoint']
slackAuthHeader = os.environ['slackAuthHeader']

print('imported all modules')


def lambda_handler(event, context):
    try:
        print('starting function')

        params = event.get('queryStringParameters', {})
        print(params)

        limit = params['limit']
        offset = params['offset']

        sparql = SPARQLWrapper(neptuneEndpoint)

        query_content = """  PREFIX rdfs: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                                PREFIX abound: <https://carrier.com/schema/1.1/Abound#>
                                PREFIX switch: <https://switchautomation.com/schemas/BrickExtension/1.0#>
                                SELECT ?assetId ?brickClass ?group
                                WHERE
                                { ?s rdfs:type ?brickClass ;
                                abound:group ?group ;
                                switch:hasUuid ?assetId ;
                                abound:isHeldBy ?nodeId .
                                }
                                """
        query_content += f' limit {limit}'
        query_content += f' offset {offset}'

        sparql.setQuery(query_content)
        print('setting format')
        sparql.setReturnFormat(JSON)
        print('about to query 2...')
        results = sparql.queryAndConvert()
        print('done querying')


        
        # 1st query INCLUDES node because node is optional and would change results if we just had this query
        query_content_with_node = """  PREFIX rdfs: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                                PREFIX abound: <https://carrier.com/schema/1.1/Abound#>
                                PREFIX switch: <https://switchautomation.com/schemas/BrickExtension/1.0#>
                                SELECT ?assetId ?brickClass ?group ?node 
                                WHERE
                                {  ?s rdfs:type ?brickClass ;
                                abound:group ?group ;
                                switch:hasUuid ?assetId ;
                                abound:isHeldBy ?nodeId .
                                ?nodeId abound:id ?node .
                                }
                                """
        

        sparql.setQuery(query_content_with_node)
        print('setting format')
        sparql.setReturnFormat(JSON)
        print('about to query 1...')
        resultsWithNode = sparql.queryAndConvert()

        print('Got here at least')
        print(sys.getsizeof(resultsWithNode))



        # Use this to match up assetId to node
        assetDict = {}
        for each in resultsWithNode['results']['bindings']:
            assetId = each['assetId']['value']
            nodeId = each['node']['value']
            assetDict[assetId] = nodeId


        

        print(sys.getsizeof(results))
        seenIds = {}
        returnArray = []
        for result in results['results']['bindings']:
            assetId = result['assetId']['value']
            if assetId not in seenIds:
                group = result['group']['value']
                brickClass = result['brickClass']['value'].split('#')[1]
                buildObj = {
                    'source': group,
                    'assetId': assetId,
                    'brickClass': brickClass
                }
                if assetId in assetDict:
                        buildObj['node'] = assetDict[assetId]
                seenIds[assetId] = True
                returnArray.append(buildObj)

        returnData = {
            'next': len(returnArray) < limit,
            'data': returnArray
        }

        print('After loop to trim: ')
        print(sys.getsizeof(returnArray))
        print('Dumps version: ')
        print(sys.getsizeof(json.dumps(returnArray)))
        return {
            'statusCode': 200,
            'body': json.dumps(returnData)
        }
    except Exception as ex:
        print('Lambda failed...')
        reportLambdaError(ex)
        return {
            'statusCode': 400,
            'body': json.dumps(ex, default=str)
        }

def reportLambdaError(error):
    slackUrlEndpoint = "https://slack.com/api/chat.postMessage"
    lambdaErrorChannel = 'C05PE0TT3MJ'
    headers = {'Authorization': slackAuthHeader, 'Content-type': 'application/json'}
    reportText = f'neptune-api lambda errored with the following error: {error}'

    finalPostParams = {
                "channel": lambdaErrorChannel,
                "text": reportText
            }
    requests.post(slackUrlEndpoint, json=finalPostParams, headers=headers, timeout=60)
