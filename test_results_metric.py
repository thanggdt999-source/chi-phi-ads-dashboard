import requests

token = 'EAASKghWluokBRDZChZAaEvZBlBQ0D1iR7oi3EzrwChPlDNer3ziIYRN92TpkOuZBIyfZBoFMZAGLqAZBNhlE6R5WUZBPcaFGjaZCFuhjtQJoP1qmf9ZCjstOlQvDWdxZAlSAmUWBrU1oeXiRFftHem4Cwavusvm5y3Et2NC60HZCiyFKo2J6AOM7ys3vXblRPopnfdlkXgZDZD'

endpoint = 'https://graph.facebook.com/v20.0/act_1405818800844826/insights'
params = {
    'fields': 'campaign_name,spend,conversions',
    'level': 'campaign',
    'date_preset': 'today',
    'access_token': token,
    'limit': 100,
}
response = requests.get(endpoint, params=params, timeout=20)
print('Status:', response.status_code)
data = response.json()
if 'data' in data:
    print(f'Found {len(data["data"])} campaigns')
    for i, c in enumerate(data['data'][:3], 1):
        print(f'\n[{i}] Campaign: {c.get("campaign_name")}')
        print(f'  spend: {c.get("spend")}')
        print(f'  conversions: {c.get("conversions")}')
else:
    print('Error:', data)
