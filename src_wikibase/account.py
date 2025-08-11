# This file contains the account information for the wikibase instance
from wikibaseintegrator import WikibaseIntegrator, wbi_login
from wikibaseintegrator.wbi_config import config
from account_usernames_passwords import *




#config['USER_AGENT'] = 'MyWikibaseBot/1.0 (https://www.wikidata.org/wiki/User:MyUsername)'
config['USER_AGENT'] = "Traopia"
# config['MEDIAWIKI_API_URL'] = wikibase_api_url 
login_wikibase = wbi_login.Login(user=WDUSER_test, password=WDPASS_test, mediawiki_api_url=wikibase_api_url)
wbi_wikibase = WikibaseIntegrator(login = login_wikibase)


#wikidata_api_url = 'https://www.wikidata.org/w/api.php'
login_wikidata = wbi_login.Login(user=WDUSER, password=WDPASS, mediawiki_api_url=wikidata_api_url)
wbi_wikidata = WikibaseIntegrator(login = login_wikidata)



