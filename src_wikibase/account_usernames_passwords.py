WDUSER_test = 'Traopia'
wikibase = "fashionwiki"

if wikibase == "wikifashion":
    WDPASS_test = 'caFtuv-9hyvfe-ruxved'
    wikibase_api_url = 'https://wikifashion.wikibase.cloud/w/api.php'

if wikibase == "fashionwiki":
    WDPASS_test = 'diqfiz-wodnI4-jafwax'
    wikibase_api_url = 'https://fashionwiki.wikibase.cloud/w/api.php'
    config = {
    "SPARQL_ENDPOINT_URL": "https://fashionwiki.wikibase.cloud/query/sparql",
    'USER_AGENT':  'YourBotName/1.0 (https://yourwebsite.org/bot-info)',#"Traopia",
    'WIKIBASE_URL': wikibase_api_url,
    }

WDUSER = 'traopia'
WDPASS = 'qysja2-murmix-kovtEp'
wikidata_api_url = 'https://www.wikidata.org/w/api.php'

openai_api_key = "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"